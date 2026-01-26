import os from 'os';
import { exec } from 'child_process';
import { promisify } from 'util';
import { calculateMethodsVersionHash } from './methodSync.js';

const execPromise = promisify(exec);

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

async function createHeartbeat(config, executor, methodsConfig) {
  if (!config || !config.NODE || !config.NODE.ID) {
    throw new Error('Invalid config: NODE.ID is required');
  }
  
  let currentMethodsConfig = methodsConfig || {};

  let encryptionManager = null;
  if (config.ENCRYPTION && config.ENCRYPTION.ENABLED) {
    try {
      // dynamic import karena ESM
      const { default: EncryptionManager } = await import('./encryption.js');
      encryptionManager = new EncryptionManager({
        ...config,
        ENCRYPTION: {
          ...config.ENCRYPTION,
          NODE_ID: config.NODE.ID
        }
      });
      console.log('[HEARTBEAT] Encryption loaded');
    } catch (error) {
      console.error('[HEARTBEAT] Failed to load encryption:', error.message);
      // Non-fatal error, continue tanpa encryption
    }
  }

  function isEncryptionEnabled() {
    return !!(encryptionManager && config.ENCRYPTION && config.ENCRYPTION.ENABLED);
  }

  async function getProcessCount() {
    try {
      if (os.platform() === 'linux') {
        const { stdout } = await execPromise('ps aux 2>/dev/null | wc -l || echo "0"');
        const count = parseInt(stdout, 10);
        return Number.isFinite(count) ? Math.max(count - 1, 0) : 0;
      } else if (os.platform() === 'win32') {
        const { stdout } = await execPromise('tasklist 2>nul | find /c /v ""');
        const count = parseInt(stdout, 10);
        return Number.isFinite(count) ? Math.max(count - 3, 0) : 0;
      } else if (os.platform() === 'darwin') {
        const { stdout } = await execPromise('ps aux 2>/dev/null | wc -l || echo "0"');
        const count = parseInt(stdout, 10);
        return Number.isFinite(count) ? Math.max(count - 1, 0) : 0;
      }
    } catch (error) {
      console.error('[HEARTBEAT] Error getting process count:', error.message);
    }
    return 0;
  }

  function updateMethodsConfig(newConfig) {
    if (newConfig && typeof newConfig === 'object') {
      currentMethodsConfig = newConfig;
      console.log(
        `[HEARTBEAT] Methods config updated: ${Object.keys(
          currentMethodsConfig
        ).length} methods`
      );
      return true;
    }
    return false;
  }

  function getMethodsVersionHash() {
    try {
      return calculateMethodsVersionHash(currentMethodsConfig);
    } catch {
      return 'unknown';
    }
  }

  async function getFullStatus() {
    try {
      const totalMem = os.totalmem();
      const freeMem = os.freemem();
      const usedMem = totalMem - freeMem;
      const memUsagePercent = ((usedMem / totalMem) * 100).toFixed(2);

      const loadAvg = os.loadavg();
      const uptime = os.uptime();
      const activeProcesses = executor.getActiveProcesses();

      const totalProcesses = await getProcessCount();

      const methodsVersion = getMethodsVersionHash();
      const methodsCount = Object.keys(currentMethodsConfig).length;

      return {
        timestamp: new Date().toISOString(),
        mode: 'DIRECT',
        node_id: config.NODE.ID,
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
        total_processes: totalProcesses,
        methods: {
          version_hash: methodsVersion,
          count: methodsCount,
          supported: Object.keys(currentMethodsConfig)
        },
        connection: {
          type: 'direct', 
          ip: config.NODE.IP || null,
          port: config.SERVER.PORT,
          encryption: !!(config.ENCRYPTION && config.ENCRYPTION.ENABLED)
        },
        node_config: {
          ip: config.NODE.IP || null,
          port: config.SERVER.PORT,
          env: config.NODE.ENV || 'production'
        }
      };
    } catch (error) {
      console.error('[HEARTBEAT] Error getting full status:', error);
      
      return {
        timestamp: new Date().toISOString(),
        mode: 'DIRECT',
        error: error.message
      };
    }
  }

  async function getSimpleStatus() {
    const status = await getFullStatus();
    return {
      node_id: config.NODE.ID,
      ...status
    };
  }

  async function autoRegister() {
    if (!config.MASTER?.URL) {
      console.log('[HEARTBEAT] MASTER_URL not configured');
      return { success: false, error: 'NO_MASTER_URL' };
    }

    try {
      const methodsVersion = getMethodsVersionHash();

      const bodyData = {
        node_id: config.NODE.ID,
        hostname: os.hostname(),
        ip: config.NODE.IP,
        port: config.SERVER.PORT,
        methods_supported: Object.keys(currentMethodsConfig),
        env: config.NODE.ENV || 'production',
        timestamp: new Date().toISOString(),
        methods_version: methodsVersion,
        methods_count: Object.keys(currentMethodsConfig).length,
        mode: 'DIRECT', 
        connection_type: 'direct',
        capabilities: {
          encryption: isEncryptionEnabled(),
          version: config.ENCRYPTION?.VERSION || 'none',
          direct_access: true,
          reverse_only: false 
        }
      };

      console.log('[HEARTBEAT] Registering to master:', {
        node_id: config.NODE.ID,
        ip: config.NODE.IP,
        port: config.SERVER.PORT,
        methods: Object.keys(currentMethodsConfig).length
      });

      let body;
      const headers = { 
        'Content-Type': 'application/json',
        'X-Node-ID': config.NODE.ID,
        'X-Node-Mode': 'DIRECT', 
      };
      
      if (isEncryptionEnabled()) {
        const encrypted = encryptionManager.createSecureMessage(bodyData, 'register');
        body = JSON.stringify(encrypted);
        headers['X-Encryption'] = 'enabled';
        headers['X-Encryption-Version'] = config.ENCRYPTION.VERSION;
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
        10000
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
          `[HEARTBEAT] Registered to master (${Object.keys(
            currentMethodsConfig
          ).length} methods, v${methodsVersion.substring(0, 8)})`
        );
        return {
          success: true,
          data,
          methodsVersion,
          methodsCount: Object.keys(currentMethodsConfig).length,
          encrypted: isEncryptionEnabled()
        };
      } else {
        const errorText = await res.text().catch(() => '');
        console.log(
          `[HEARTBEAT] Register failed: HTTP ${res.status} - ${errorText}`
        );
        return {
          success: false,
          error: `HTTP ${res.status}`,
          status: res.status
        };
      }
    } catch (err) {
      console.error('[HEARTBEAT] Register error:', err.message);
      return {
        success: false,
        error: err.message
      };
    }
  }

  async function sendHeartbeat() {
    if (!config.MASTER?.URL) {
      console.log(
        '[HEARTBEAT] MASTER_URL not configured'
      );
      return { success: false, error: 'NO_MASTER_URL' };
    }

    try {
      const status = await getFullStatus();
      
      const { node_id, ...statusWithoutNodeId } = status;
      
      const requestBody = {
        node_id: config.NODE.ID,
        ...statusWithoutNodeId
      };
      
      console.log('[HEARTBEAT] Sending heartbeat with node_id:', config.NODE.ID);
      console.log('[HEARTBEAT] Mode set to:', status.mode);
      console.log('[HEARTBEAT] Connection type:', status.connection?.type);
      console.log('[HEARTBEAT] Status keys:', Object.keys(status));

      let body;
      const headers = { 
        'Content-Type': 'application/json',
        'X-Node-ID': config.NODE.ID,
        'X-Node-Mode': 'DIRECT', 
        'X-Connection-Type': 'direct'
      };
      
      if (isEncryptionEnabled()) {
        const encrypted = encryptionManager.createSecureMessage(requestBody, 'heartbeat');
        body = JSON.stringify(encrypted);
        headers['X-Encryption'] = 'enabled';
        headers['X-Encryption-Version'] = config.ENCRYPTION.VERSION;
        console.log('[HEARTBEAT] Sending ENCRYPTED heartbeat');
      } else {
        body = JSON.stringify(requestBody);
        console.log('[HEARTBEAT] Sending PLAIN heartbeat');
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

      if (res.ok) {
        console.log('[HEARTBEAT] ✓ Heartbeat sent successfully');
        return { success: true };
      } else {
        const errorText = await res.text().catch(() => '');
        console.log(
          `[HEARTBEAT] ✗ Heartbeat failed: HTTP ${res.status} - ${errorText}`
        );
        
        if (!isEncryptionEnabled()) {
          console.log('[HEARTBEAT] Request body sent:', JSON.stringify(requestBody, null, 2));
        }
        
        return { 
          success: false, 
          error: `HTTP ${res.status}`,
          status: res.status
        };
      }
    } catch (err) {
      console.error('[HEARTBEAT] Heartbeat error:', err.message);
      return {
        success: false,
        error: err.message
      };
    }
  }

  async function updateMethodsVersion() {
    if (!config.MASTER?.URL) {
      console.log(
        '[HEARTBEAT] MASTER_URL not configured'
      );
      return { success: false, error: 'NO_MASTER_URL' };
    }

    try {
      const methodsVersion = getMethodsVersionHash();
      const methodsCount = Object.keys(currentMethodsConfig).length;

      const bodyData = {
        node_id: config.NODE.ID,
        methods_version_hash: methodsVersion,
        methods_count: methodsCount,
        timestamp: new Date().toISOString()
      };

      let body;
      const headers = { 
        'Content-Type': 'application/json',
        'X-Node-ID': config.NODE.ID
      };
      
      if (isEncryptionEnabled()) {
        const encrypted = encryptionManager.createSecureMessage(bodyData, 'methods_update');
        body = JSON.stringify(encrypted);
        headers['X-Encryption'] = 'enabled';
        headers['X-Encryption-Version'] = config.ENCRYPTION.VERSION;
      } else {
        body = JSON.stringify(bodyData);
      }

      const res = await fetchWithTimeout(
        `${config.MASTER.URL}/node-methods-updated`,
        {
          method: 'POST',
          headers,
          body
        },
        10000
      );

      if (res.ok) {
        console.log(
          `[HEARTBEAT] Updated methods on master: ${methodsCount} methods (v${methodsVersion.substring(
            0,
            8
          )})`
        );
        return { success: true };
      } else {
        const errorText = await res.text().catch(() => '');
        console.log(
          `[HEARTBEAT] Update methods failed: HTTP ${res.status} - ${errorText}`
        );
        return {
          success: false,
          error: `HTTP ${res.status}`,
          status: res.status
        };
      }
    } catch (err) {
      console.error(
        '[HEARTBEAT] Update methods error:',
        err.message
      );
      return {
        success: false,
        error: err.message
      };
    }
  }

  async function sendEncryptedHeartbeat() {
    return sendHeartbeat();
  }

  function startBackgroundHeartbeat(interval = 30000) {
    let heartbeatInterval = null;

    async function executeHeartbeat() {
      try {
        console.log('[HEARTBEAT] Executing background heartbeat...');
        const result = await sendHeartbeat();
        if (result.success) {
          console.log('[HEARTBEAT] ✓ Background heartbeat successful');
        } else {
          console.error(`[HEARTBEAT] ✗ Background heartbeat failed:`, result.error);
          console.log(`[HEARTBEAT] Will retry in ${interval/1000} seconds...`);
        }
      } catch (error) {
        console.error(`[HEARTBEAT] ✗ Background heartbeat error:`, error.message);
        console.log(`[HEARTBEAT] Will retry in ${interval/1000} seconds...`);
      }
    }

    function start() {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
      }
      
      console.log(`[HEARTBEAT] Starting INFINITE RETRY background heartbeat every ${interval}ms`);
      
      setTimeout(executeHeartbeat, 2000);
      
      heartbeatInterval = setInterval(executeHeartbeat, interval);
    }

    function stop() {
      if (heartbeatInterval) {
        clearInterval(heartbeatInterval);
        heartbeatInterval = null;
        console.log('[HEARTBEAT] Stopped background heartbeat');
      }
    }

    return {
      start,
      stop,
      execute: executeHeartbeat
    };
  }

  return {
    autoRegister,
    sendHeartbeat,
    sendEncryptedHeartbeat,
    getSimpleStatus,
    getFullStatus,
    updateMethodsVersion,
    updateMethodsConfig,
    getMethodsCount: () => Object.keys(currentMethodsConfig).length,
    getMethodsVersion: getMethodsVersionHash,
    isEncryptionEnabled,
    startBackgroundHeartbeat,
    getProcessCount
  };
}

export default createHeartbeat;
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { promisify } from 'util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const writeFileAsync = promisify(fs.writeFile);
const renameAsync = promisify(fs.rename);
const unlinkAsync = promisify(fs.unlink);

function createProxyUpdater(config) {
  let updateLock = null;
  const lockTimeout = 300000; // 5 minutes max lock time
  let isRunning = false;

  const proxyFile = path.join(__dirname, '..', 'proxy.txt');
  const tempFile = path.join(__dirname, '..', 'proxy.txt.tmp');

  // FIX: Use sources from config instead of duplicating the list here
  const proxySources = config?.PROXY?.SOURCES || [];

  if (proxySources.length === 0) {
    console.warn('[PROXY] No proxy sources configured in config.PROXY.SOURCES');
  }

  function acquireLock() {
    const now = Date.now();

    if (updateLock) {
      const lockAge = now - updateLock;
      if (lockAge < lockTimeout) {
        return false;
      }
      console.log(`[PROXY] Stale lock detected (${Math.round(lockAge / 1000)}s old), clearing`);
      updateLock = null;
    }

    updateLock = now;
    return true;
  }

  function releaseLock() {
    updateLock = null;
  }

  function isValidProxyIP(ip) {
    const parts = ip.split('.');

    if (parts.length !== 4) return false;

    for (const part of parts) {
      const num = parseInt(part, 10);
      if (isNaN(num) || num < 0 || num > 255) return false;
    }

    const first = parseInt(parts[0], 10);
    const second = parseInt(parts[1], 10);

    if (first === 0) return false;
    if (first === 10) return false;
    if (first === 127) return false;
    if (first === 169 && second === 254) return false;
    if (first === 172 && second >= 16 && second <= 31) return false;
    if (first === 192 && second === 168) return false;
    if (first >= 224 && first <= 239) return false;
    if (first >= 240) return false;

    return true;
  }

  function isValidProxy(ip, port) {
    if (!isValidProxyIP(ip)) return false;

    const portNum = parseInt(port, 10);
    if (isNaN(portNum) || portNum < 1 || portNum > 65535) return false;

    const bannedPorts = [22, 23, 25, 443, 465, 587, 993, 995];
    if (bannedPorts.includes(portNum)) return false;

    return true;
  }

  async function downloadProxies(sourceUrl) {
    try {
      console.log(`[PROXY] Downloading from: ${sourceUrl}`);

      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 60000);

      try {
        const response = await globalThis.fetch(sourceUrl, {
          signal: controller.signal,
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
          }
        });

        clearTimeout(timeout);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let result = '';
        let chunk;

        while (!(chunk = await reader.read()).done) {
          result += decoder.decode(chunk.value, { stream: true });

          if (result.length > 10 * 1024 * 1024) {
            console.log('[PROXY] Response too large, truncating');
            break;
          }
        }

        return result;
      } catch (error) {
        clearTimeout(timeout);
        throw error;
      }
    } catch (error) {
      console.error(`[PROXY] Failed to download from ${sourceUrl}: ${error.message}`);
      return null;
    }
  }

  function parseProxies(proxyText) {
    if (!proxyText) return [];

    const proxies = new Set();
    let validCount = 0;
    let invalidCount = 0;

    // FIX: Split once, iterate without mutating array (prevents index reset bug)
    const lines = proxyText.split('\n');

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();

      if (!line || line.startsWith('#') || line.startsWith('//')) continue;

      const match = line.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})/);
      if (match) {
        const ip = match[1];
        const port = match[2];

        if (isValidProxy(ip, port)) {
          proxies.add(`${ip}:${port}`);
          validCount++;
        } else {
          invalidCount++;
        }
      }
    }

    console.log(`[PROXY] Parsed: ${validCount} valid, ${invalidCount} invalid`);

    return Array.from(proxies);
  }

  async function updateProxyFile() {
    if (!acquireLock()) {
      console.log('[PROXY] Update already running (locked), skip');
      return { success: false, error: 'Update in progress' };
    }

    // FIX: Set isRunning AFTER acquiring lock (not before releasing it in finally)
    isRunning = true;

    try {
      console.log('[PROXY] Updating proxy.txt from all sources...');

      const allProxies = [];
      const sourceResults = {
        success: 0,
        failed: 0,
        total: proxySources.length
      };

      for (let i = 0; i < proxySources.length; i++) {
        const source = proxySources[i];

        try {
          const data = await downloadProxies(source);

          if (data) {
            const proxies = parseProxies(data);
            console.log(`[PROXY] ✓ Got ${proxies.length} from source ${i + 1}/${proxySources.length}`);

            for (let j = 0; j < proxies.length; j += 1000) {
              const batch = proxies.slice(j, j + 1000);
              allProxies.push(...batch);
            }

            sourceResults.success++;
          } else {
            console.log(`[PROXY] ✗ Source ${i + 1}/${proxySources.length} failed`);
            sourceResults.failed++;
          }
        } catch (err) {
          console.log(`[PROXY] ✗ Source ${i + 1} error: ${err.message}`);
          sourceResults.failed++;
        }

        if (i < proxySources.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }

        if (global.gc && i % 3 === 0) {
          global.gc();
        }
      }

      console.log('[PROXY] Deduplicating proxies...');
      const uniqueProxies = [...new Set(allProxies)];

      // Free memory
      allProxies.length = 0;

      if (uniqueProxies.length === 0) {
        console.log('[PROXY] ✗ No proxies found from any source');
        return {
          success: false,
          error: 'No proxies found',
          sourceResults
        };
      }

      const content = uniqueProxies.join('\n');

      try {
        await writeFileAsync(tempFile, content, 'utf8');
        console.log('[PROXY] ✓ Wrote to temporary file');

        if (!fs.existsSync(tempFile)) {
          throw new Error('Temp file not created');
        }

        const tempStats = fs.statSync(tempFile);
        if (tempStats.size === 0) {
          throw new Error('Temp file is empty');
        }

        await renameAsync(tempFile, proxyFile);
        console.log('[PROXY] ✓ Atomically replaced proxy file');

      } catch (writeError) {
        console.error('[PROXY] ✗ Failed to write proxy file:', writeError.message);

        try {
          if (fs.existsSync(tempFile)) {
            await unlinkAsync(tempFile);
          }
        } catch (cleanupErr) {
          console.error('[PROXY] Failed to cleanup temp file:', cleanupErr.message);
        }

        return {
          success: false,
          error: `Write failed: ${writeError.message}`,
          sourceResults
        };
      }

      console.log(`[PROXY] ✓ Updated! ${uniqueProxies.length} proxies saved`);
      console.log(`[PROXY] Sources: ${sourceResults.success} success, ${sourceResults.failed} failed`);

      return {
        success: true,
        count: uniqueProxies.length,
        sources: sourceResults,
        timestamp: new Date().toISOString()
      };

    } catch (error) {
      console.error('[PROXY] ✗ Update failed:', error.message);
      return { success: false, error: error.message };
    } finally {
      // FIX: Always release lock and clear isRunning in finally
      isRunning = false;
      releaseLock();
    }
  }

  let updateInterval = null;

  function startAutoUpdate(intervalMinutes = 10, executor = null) {
    stopAutoUpdate();

    const intervalMs = intervalMinutes * 60 * 1000;

    function attackInProgress() {
      if (!executor) return false;
      try {
        return executor.getActiveProcessesCount() > 0;
      } catch {
        return false;
      }
    }

    console.log(`[PROXY] Auto-update every ${intervalMinutes} minutes (attack-guard: ${executor ? 'enabled' : 'disabled'})`);

    if (attackInProgress()) {
      console.log('[PROXY] ⚠ Initial update skipped — attack in progress. Will retry at next interval.');
    } else {
      updateProxyFile()
        .then(result => {
          if (result.success) {
            console.log(`[PROXY] Initial update: ${result.count} proxies`);
          } else {
            console.log(`[PROXY] Initial update failed: ${result.error}`);
          }
        })
        .catch(error => {
          console.error('[PROXY] Initial update error:', error.message);
        });
    }

    updateInterval = setInterval(() => {
      if (attackInProgress()) {
        const activeCount = executor.getActiveProcessesCount();
        console.log(
          `[PROXY] ⚠ Auto-update skipped — ${activeCount} active attack process(es) running. ` +
          `Will retry in ${intervalMinutes} minute(s).`
        );
        return;
      }

      console.log('[PROXY] Auto-update triggered');
      updateProxyFile().catch(error => {
        console.error('[PROXY] Auto-update error:', error.message);
      });
    }, intervalMs);
  }

  function stopAutoUpdate() {
    if (updateInterval) {
      clearInterval(updateInterval);
      updateInterval = null;
      console.log('[PROXY] Auto-update stopped');
    }
  }

  function getProxyCount() {
    try {
      if (fs.existsSync(proxyFile)) {
        const content = fs.readFileSync(proxyFile, 'utf8');
        return content.split('\n').filter(line => line.trim() !== '').length;
      }
      return 0;
    } catch (error) {
      console.error('[PROXY] Error getting proxy count:', error.message);
      return 0;
    }
  }

  function getProxyList(limit = 0) {
    try {
      if (!fs.existsSync(proxyFile)) return [];

      const content = fs.readFileSync(proxyFile, 'utf8');
      const proxies = content.split('\n')
        .filter(line => line.trim() !== '')
        .map(line => line.trim());

      return limit > 0 ? proxies.slice(0, limit) : proxies;
    } catch (error) {
      console.error('[PROXY] Error getting proxy list:', error.message);
      return [];
    }
  }

  async function manualUpdate() {
    return updateProxyFile();
  }

  function getUpdateStatus() {
    return {
      running: isRunning,
      locked: updateLock !== null,
      lockAge: updateLock ? Date.now() - updateLock : null,
      autoUpdateEnabled: updateInterval !== null,
      proxyCount: getProxyCount(),
      proxyFileExists: fs.existsSync(proxyFile),
      tempFileExists: fs.existsSync(tempFile)
    };
  }

  function forceUnlock() {
    if (updateLock) {
      const lockAge = Date.now() - updateLock;
      console.log(`[PROXY] Force unlocking (lock age: ${Math.round(lockAge / 1000)}s)`);
      releaseLock();
      isRunning = false;
      return true;
    }
    return false;
  }

  function cleanupTempFiles() {
    try {
      if (fs.existsSync(tempFile)) {
        fs.unlinkSync(tempFile);
        console.log('[PROXY] Cleaned up temp file');
        return true;
      }
      return false;
    } catch (error) {
      console.error('[PROXY] Failed to cleanup temp file:', error.message);
      return false;
    }
  }

  return {
    updateProxyFile,
    startAutoUpdate,
    stopAutoUpdate,
    manualUpdate,
    getProxyCount,
    getProxyList,
    getUpdateStatus,
    forceUnlock,
    cleanupTempFiles,
    isUpdating: () => isRunning,
    isLocked: () => updateLock !== null
  };
}

export default createProxyUpdater;
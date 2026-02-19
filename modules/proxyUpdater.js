import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { promisify } from 'util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const writeFileAtomic = promisify(fs.writeFile);
const renameAtomic = promisify(fs.rename);
const unlinkAtomic = promisify(fs.unlink);

function createProxyUpdater(config) {
  // FIX #44: Use lock object with timestamp
  let updateLock = null;
  const lockTimeout = 300000; // 5 minutes max lock time
  let isRunning = false;
  
  const proxyFile = path.join(__dirname, '..', 'proxy.txt');
  const tempFile = path.join(__dirname, '..', 'proxy.txt.tmp');
  
  const defaultSources = [
    'https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/refs/heads/main/http/raw/all.txt',
    'https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/refs/heads/main/socks4/raw/all.txt',
    'https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/refs/heads/main/socks5/raw/all.txt',
    'https://raw.githubusercontent.com/elliottophellia/proxylist/refs/heads/master/results/http/global/http_checked.txt',
    'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt',
    'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
    'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt',
    'https://raw.githubusercontent.com/gitrecon1455/fresh-proxy-list/refs/heads/main/proxylist.txt'
  ];
  
  const proxySources = config?.PROXY?.SOURCES || defaultSources;
  
  // FIX #44: Acquire lock with timeout check
  function acquireLock() {
    const now = Date.now();
    
    if (updateLock) {
      const lockAge = now - updateLock;
      if (lockAge < lockTimeout) {
        return false; // Lock still valid
      } else {
        console.log(`[PROXY] Stale lock detected (${Math.round(lockAge/1000)}s old), clearing`);
        updateLock = null;
      }
    }
    
    updateLock = now;
    return true;
  }
  
  function releaseLock() {
    updateLock = null;
  }
  
  // FIX #47: Validate proxy IP address
  function isValidProxyIP(ip) {
    const parts = ip.split('.');
    
    if (parts.length !== 4) return false;
    
    for (const part of parts) {
      const num = parseInt(part, 10);
      if (isNaN(num) || num < 0 || num > 255) return false;
    }
    
    // Reject private/reserved IP ranges
    const first = parseInt(parts[0], 10);
    const second = parseInt(parts[1], 10);
    
    // 0.0.0.0/8 - Current network
    if (first === 0) return false;
    
    // 10.0.0.0/8 - Private network
    if (first === 10) return false;
    
    // 127.0.0.0/8 - Loopback
    if (first === 127) return false;
    
    // 169.254.0.0/16 - Link-local
    if (first === 169 && second === 254) return false;
    
    // 172.16.0.0/12 - Private network
    if (first === 172 && second >= 16 && second <= 31) return false;
    
    // 192.168.0.0/16 - Private network
    if (first === 192 && second === 168) return false;
    
    // 224.0.0.0/4 - Multicast
    if (first >= 224 && first <= 239) return false;
    
    // 240.0.0.0/4 - Reserved
    if (first >= 240) return false;
    
    return true;
  }
  
  // FIX #47: Improved proxy validation
  function isValidProxy(ip, port) {
    // Validate IP
    if (!isValidProxyIP(ip)) {
      return false;
    }
    
    // Validate port
    const portNum = parseInt(port, 10);
    if (isNaN(portNum) || portNum < 1 || portNum > 65535) {
      return false;
    }
    
    // Reject common non-proxy ports
    const bannedPorts = [22, 23, 25, 443, 465, 587, 993, 995];
    if (bannedPorts.includes(portNum)) {
      return false;
    }
    
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
        
        // FIX #45: Stream response in chunks to avoid memory spike
        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let result = '';
        let chunk;
        
        while (!(chunk = await reader.read()).done) {
          result += decoder.decode(chunk.value, { stream: true });
          
          // FIX #45: Limit max size to prevent memory issues
          if (result.length > 10 * 1024 * 1024) { // 10MB max
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
  
  // FIX #12: parseProxies rewritten to NOT mutate array during iteration.
  // Original code used lines.splice(0, i) inside the loop which caused index
  // to reset to 0 and skip lines that hadn't been processed yet.
  function parseProxies(proxyText) {
    if (!proxyText) return [];
    
    const proxies = new Set();
    let validCount = 0;
    let invalidCount = 0;
    
    // FIX #12: Split once, iterate without mutating
    const lines = proxyText.split('\n');
    
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      
      if (!line || line.startsWith('#') || line.startsWith('//')) continue;
      
      // Extract IP:PORT pattern
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
      
      // FIX #12: Instead of splicing the array (which breaks iteration),
      // periodically hint GC by releasing processed segment via a separate counter.
      // The lines array is NOT mutated — we just let it be GC'd naturally after the loop.
    }
    
    console.log(`[PROXY] Parsed: ${validCount} valid, ${invalidCount} invalid`);
    
    return Array.from(proxies);
  }
  
  async function updateProxyFile() {
    // FIX #44: Proper lock acquisition
    if (!acquireLock()) {
      console.log('[PROXY] Update already running (locked), skip');
      return { success: false, error: 'Update in progress' };
    }
    
    try {
      isRunning = true;
      console.log('[PROXY] Updating proxy.txt from all sources...');
      
      const allProxies = [];
      const sourceResults = {
        success: 0,
        failed: 0,
        total: proxySources.length
      };
      
      // FIX #45: Process sources sequentially with memory management
      for (let i = 0; i < proxySources.length; i++) {
        const source = proxySources[i];
        
        try {
          const data = await downloadProxies(source);
          
          if (data) {
            const proxies = parseProxies(data);
            console.log(`[PROXY] ✓ Got ${proxies.length} from source ${i + 1}/${proxySources.length}`);
            
            // FIX #45: Add in batches to avoid huge array concat
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
        
        // Delay between sources to avoid rate limiting
        if (i < proxySources.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
        
        // FIX #45: Force garbage collection hint after each source
        if (global.gc && i % 3 === 0) {
          global.gc();
        }
      }
      
      // FIX #45: Deduplicate efficiently using Set
      console.log('[PROXY] Deduplicating proxies...');
      const uniqueProxies = [...new Set(allProxies)];
      
      // Clear original array to free memory
      allProxies.length = 0;
      
      if (uniqueProxies.length === 0) {
        console.log('[PROXY] ✗ No proxies found from any source');
        releaseLock();
        return { 
          success: false, 
          error: 'No proxies found',
          sourceResults
        };
      }
      
      // FIX #46: Write to temp file first, then atomic rename
      const content = uniqueProxies.join('\n');
      
      try {
        // Write to temporary file
        await writeFileAtomic(tempFile, content, 'utf8');
        console.log('[PROXY] ✓ Wrote to temporary file');
        
        // Verify temp file
        if (!fs.existsSync(tempFile)) {
          throw new Error('Temp file not created');
        }
        
        const tempStats = fs.statSync(tempFile);
        if (tempStats.size === 0) {
          throw new Error('Temp file is empty');
        }
        
        // Atomic rename (replaces old file)
        await renameAtomic(tempFile, proxyFile);
        console.log('[PROXY] ✓ Atomically replaced proxy file');
        
      } catch (writeError) {
        console.error('[PROXY] ✗ Failed to write proxy file:', writeError.message);
        
        // Cleanup temp file if it exists
        try {
          if (fs.existsSync(tempFile)) {
            await unlinkAtomic(tempFile);
          }
        } catch (cleanupErr) {
          console.error('[PROXY] Failed to cleanup temp file:', cleanupErr.message);
        }
        
        releaseLock();
        return {
          success: false,
          error: `Write failed: ${writeError.message}`,
          sourceResults
        };
      }
      
      console.log(`[PROXY] ✓ Updated! ${uniqueProxies.length} proxies saved`);
      console.log(`[PROXY] Sources: ${sourceResults.success} success, ${sourceResults.failed} failed`);
      
      releaseLock();
      return { 
        success: true, 
        count: uniqueProxies.length,
        sources: sourceResults,
        timestamp: new Date().toISOString()
      };
      
    } catch (error) {
      console.error('[PROXY] ✗ Update failed:', error.message);
      console.error('[PROXY] Stack trace:', error.stack);
      releaseLock();
      return { success: false, error: error.message };
    } finally {
      isRunning = false;
    }
  }
  
  let updateInterval = null;

  /**
   * Start periodic proxy auto-update.
   *
   * @param {number} intervalMinutes - Update interval in minutes.
   * @param {object|null} executor   - Optional executor instance. When provided,
   *                                   the update is skipped if there are active
   *                                   attack processes running to ensure stability.
   */
  function startAutoUpdate(intervalMinutes = 10, executor = null) {
    stopAutoUpdate();
    
    const intervalMs = intervalMinutes * 60 * 1000;

    // Helper: check whether an attack is currently in progress
    function attackInProgress() {
      if (!executor) return false;
      try {
        return executor.getActiveProcessesCount() > 0;
      } catch {
        return false;
      }
    }
    
    console.log(`[PROXY] Auto-update every ${intervalMinutes} minutes (attack-guard: ${executor ? 'enabled' : 'disabled'})`);
    
    // Initial update with error handling
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
    
    // Interval with error handling and attack guard
    updateInterval = setInterval(() => {
      // ── ATTACK GUARD ──────────────────────────────────────────────────────
      if (attackInProgress()) {
        const activeCount = executor.getActiveProcessesCount();
        console.log(
          `[PROXY] ⚠ Auto-update skipped — ${activeCount} active attack process(es) running. ` +
          `Will retry in ${intervalMinutes} minute(s).`
        );
        return;
      }
      // ──────────────────────────────────────────────────────────────────────

      console.log(`[PROXY] Auto-update triggered`);
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
    return await updateProxyFile();
  }
  
  // Get update status
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
  
  // Force unlock (use with caution)
  function forceUnlock() {
    if (updateLock) {
      const lockAge = Date.now() - updateLock;
      console.log(`[PROXY] Force unlocking (lock age: ${Math.round(lockAge/1000)}s)`);
      releaseLock();
      isRunning = false;
      return true;
    }
    return false;
  }
  
  // Cleanup temp files
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
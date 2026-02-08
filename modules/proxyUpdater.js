import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function createProxyUpdater(config) {
  let updateInterval = null;
  let isRunning = false;
  const proxyFile = path.join(__dirname, '..', 'proxy.txt');
  
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
        
        return await response.text();
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
    
    const lines = proxyText.split('\n');
    for (let line of lines) {
      line = line.trim();
      
      if (!line || line.startsWith('#') || line.startsWith('//')) continue;
      
      const match = line.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{1,5})/);
      if (match) {
        const ip = match[1];
        const port = match[2];
        
        const ipValid = ip.split('.').every(part => {
          const num = parseInt(part);
          return num >= 0 && num <= 255;
        });
        
        const portNum = parseInt(port);
        const portValid = portNum >= 1 && portNum <= 65535;
        
        if (ipValid && portValid) {
          proxies.add(`${ip}:${port}`);
        }
      }
    }
    
    return Array.from(proxies);
  }
  
  async function updateProxyFile() {
    // FIX #1: Prevent concurrent updates
    if (isRunning) {
      console.log('[PROXY] Update already running, skip');
      return { success: false, error: 'Already running' };
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
      
      // FIX #2: Process sources sequentially to avoid stack overflow
      for (let i = 0; i < proxySources.length; i++) {
        const source = proxySources[i];
        
        try {
          const data = await downloadProxies(source);
          
          if (data) {
            const proxies = parseProxies(data);
            console.log(`[PROXY] ✓ Got ${proxies.length} from source ${i + 1}/${proxySources.length}`);
            allProxies.push(...proxies);
            sourceResults.success++;
          } else {
            console.log(`[PROXY] ✗ Source ${i + 1}/${proxySources.length} failed`);
            sourceResults.failed++;
          }
        } catch (err) {
          console.log(`[PROXY] ✗ Source ${i + 1} error: ${err.message}`);
          sourceResults.failed++;
        }
        
        // FIX #3: Add delay between sources to avoid rate limiting
        if (i < proxySources.length - 1) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }
      }
      
      // FIX #4: Deduplicate proxies efficiently
      const uniqueProxies = [...new Set(allProxies)];
      
      if (uniqueProxies.length === 0) {
        console.log('[PROXY] ✗ No proxies found from any source');
        return { 
          success: false, 
          error: 'No proxies found',
          sourceResults
        };
      }
      
      // FIX #5: Write to file safely
      const content = uniqueProxies.join('\n');
      try {
        fs.writeFileSync(proxyFile, content, 'utf8');
      } catch (writeError) {
        console.error('[PROXY] ✗ Failed to write proxy file:', writeError.message);
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
      console.error('[PROXY] Stack trace:', error.stack);
      return { success: false, error: error.message };
    } finally {
      isRunning = false;
    }
  }
  
  function startAutoUpdate(intervalMinutes = 10) {
    stopAutoUpdate();
    
    const intervalMs = intervalMinutes * 60 * 1000;
    
    console.log(`[PROXY] Auto-update every ${intervalMinutes} minutes`);
    
    // FIX #6: Initial update with error handling
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
    
    // FIX #7: Interval with error handling
    updateInterval = setInterval(() => {
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
  
  return {
    updateProxyFile,
    startAutoUpdate,
    stopAutoUpdate,
    manualUpdate,
    getProxyCount,
    getProxyList,
    isUpdating: () => isRunning
  };
}

export default createProxyUpdater;
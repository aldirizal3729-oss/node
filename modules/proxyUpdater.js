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
      const timeout = setTimeout(() => controller.abort(), 60000); // 60s timeout
      
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
      console.error(`[PROXY] Failed to download: ${error.message}`);
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
    if (isRunning) {
      console.log('[PROXY] Update already running, skip');
      return { success: false, error: 'Already running' };
    }
    
    try {
      isRunning = true;
      console.log('[PROXY] Updating proxy.txt... (NO LIMIT)');
      
      const allProxies = [];
      
      // FIX: NO LIMIT - Download from ALL sources
      for (const source of proxySources) {
        try {
          const data = await downloadProxies(source);
          if (data) {
            const proxies = parseProxies(data);
            console.log(`[PROXY] Got ${proxies.length} from ${source.substring(0, 60)}...`);
            allProxies.push(...proxies);
            
            // NO LIMIT REMOVED!
            // Continue to ALL sources
          }
        } catch (err) {
          console.log(`[PROXY] Source ${source} failed: ${err.message}`);
        }
        
        // Small delay between sources to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      
      const uniqueProxies = [...new Set(allProxies)];
      
      if (uniqueProxies.length === 0) {
        console.log('[PROXY] No proxies found');
        return { success: false, error: 'No proxies found' };
      }
      
      const content = uniqueProxies.join('\n');
      fs.writeFileSync(proxyFile, content, 'utf8');
      
      console.log(`[PROXY] ✓ Updated! ${uniqueProxies.length} proxies saved to ${proxyFile}`);
      
      return { 
        success: true, 
        count: uniqueProxies.length,
        sources: proxySources.length,
        timestamp: new Date().toISOString()
      };
      
    } catch (error) {
      console.error('[PROXY] Update failed:', error);
      return { success: false, error: error.message };
    } finally {
      isRunning = false;
    }
  }
  
  function startAutoUpdate(intervalMinutes = 10) {
    stopAutoUpdate();
    
    const intervalMs = intervalMinutes * 60 * 1000;
    
    console.log(`[PROXY] Auto-update every ${intervalMinutes} minutes (NO LIMIT)`);
    
    updateProxyFile().then(result => {
      if (result.success) {
        console.log(`[PROXY] Initial: ${result.count} proxies from ${result.sources} sources`);
      }
    });
    
    updateInterval = setInterval(() => {
      console.log(`[PROXY] Auto-update triggered (NO LIMIT)`);
      updateProxyFile();
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
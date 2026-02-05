import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// MUTEX for sync operations
let syncInProgress = false;
let syncQueue = [];

function log(...args) {
  console.log('[METHOD-SYNC]', ...args);
}

function escapeRegExp(string) {
  return string.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function extractPotentialFiles(text) {
  if (!text || typeof text !== 'string') return [];
  
  const files = new Set();
  const words = text.split(/\s+/);
  
  for (const word of words) {
    let cleanWord = word
      .replace(/^["']|["']$/g, '')
      .replace(/^\.\//, '')
      .replace(/^\.\.\//, '');
    
    if (
      cleanWord.length < 2 || 
      cleanWord.includes('{') || 
      cleanWord.includes('://') ||
      /^\d+$/.test(cleanWord) ||
      isSystemCommand(cleanWord)
    ) {
      continue;
    }
    
    if (!cleanWord.includes('/') && !cleanWord.includes('\\')) {
      const fileName = path.basename(cleanWord);
      if (fileName && fileName.length >= 2) {
        const wordPattern = new RegExp(`(^|\\s)${escapeRegExp(word)}($|\\s)`);
        if (wordPattern.test(text)) {
          files.add(fileName);
        }
      }
    }
  }
  
  return Array.from(files);
}

function isSystemCommand(cmd) {
  const commands = [
    'python3', 'python', 'node', 'bash', 'sh', 'perl', 'ruby', 'php',
    'java', 'go', 'echo', 'curl', 'wget', 'cat', 'gcc', 'g++', 'make',
    'chmod', 'mkdir', 'rm', 'cp', 'mv', 'ls', 'pwd', 'cd', 'sleep',
    'timeout', 'kill', 'ps', 'grep', 'awk', 'sed', 'find', 'tar',
    'zip', 'unzip', 'gunzip'
  ];
  return commands.includes(cmd);
}

export function calculateMethodsVersionHash(methodsConfig) {
  try {
    const obj = methodsConfig || {};
    const sortedKeys = Object.keys(obj).sort();
    const normalized = JSON.stringify(
      obj,
      sortedKeys.length ? sortedKeys : undefined
    );
    return crypto.createHash('sha256').update(normalized).digest('hex');
  } catch (e) {
    console.error('[METHOD-SYNC] Error calculating methods hash:', e.message);
    return 'unknown';
  }
}

export function getCurrentMethodsVersionHash(methodsConfig) {
  return calculateMethodsVersionHash(methodsConfig);
}

// IMPROVED: Process sync queue immediately
async function processSyncQueue() {
  if (syncQueue.length === 0) return;
  
  const pendingSync = syncQueue.shift();
  const { config, resolve, reject } = pendingSync;
  
  try {
    const result = await performSync(config);
    resolve(result);
  } catch (error) {
    reject(error);
  }
  
  // Process next in queue immediately
  if (syncQueue.length > 0) {
    setImmediate(() => processSyncQueue());
  }
}

// IMPROVED: Main sync function with instant P2P propagation
async function performSync(config) {
  log('Starting method sync...');
  
  if (!config.MASTER?.URL) {
    return { success: false, error: 'Master URL not configured' };
  }
  
  const methodsJsonPath = config.SERVER.METHODS_PATH;
  const dataDir = path.join(__dirname, '..', 'lib', 'data');
  
  // Ensure directories exist
  [path.dirname(methodsJsonPath), dataDir].forEach(dir => {
    try {
      fs.mkdirSync(dir, { recursive: true });
    } catch (err) {
      if (err.code !== 'EEXIST') {
        log(`Warning: Could not create directory ${dir}: ${err.message}`);
      }
    }
  });

  log('Fetching methods from master...');
  
  const controller1 = new AbortController();
  let timeout1 = null;
  
  try {
    timeout1 = setTimeout(() => controller1.abort(), 10000);
    
    const response = await globalThis.fetch(`${config.MASTER.URL}/methods`, {
      signal: controller1.signal
    });
    
    clearTimeout(timeout1);
    timeout1 = null;
    
    if (!response.ok) {
      throw new Error(`Master response: ${response.status} ${response.statusText}`);
    }
    
    const remoteData = await response.json();
    
    if (!remoteData?.methods) {
      throw new Error('No methods in response');
    }
    
    const remoteMethods = remoteData.methods;
    const remoteHash = remoteData.version_hash || 'unknown';
    
    log(`Received ${Object.keys(remoteMethods).length} methods from master`);
    
    // Collect files
    log('Collecting files from methods...');
    const allFiles = new Set();
    const methodFiles = {};
    
    for (const [methodName, methodConfig] of Object.entries(remoteMethods)) {
      if (methodConfig.cmd) {
        const files = extractPotentialFiles(methodConfig.cmd);
        methodFiles[methodName] = files;
        files.forEach(file => allFiles.add(file));
        
        if (files.length > 0) {
          log(`  ${methodName}: ${files.join(', ')}`);
        }
      }
    }
    
    const fileList = Array.from(allFiles);
    log(`Found ${fileList.length} unique files to check`);
    
    // Download files in parallel (IMPROVED: increased concurrency)
    const downloaded = [];
    const skipped = [];
    const failed = [];
    
    // Process files in batches of 5 for faster download
    const batchSize = 5;
    for (let i = 0; i < fileList.length; i += batchSize) {
      const batch = fileList.slice(i, i + batchSize);
      
      await Promise.all(batch.map(async (filename) => {
        const filePath = path.join(dataDir, filename);
        
        // Skip if file exists and has content
        if (fs.existsSync(filePath)) {
          const stats = fs.statSync(filePath);
          if (stats.size > 0) {
            log(`✓ ${filename} already exists (${stats.size} bytes)`);
            skipped.push({ filename, size: stats.size });
            return;
          }
        }
        
        try {
          log(`Downloading ${filename}...`);
          
          const controller2 = new AbortController();
          const timeout2 = setTimeout(() => controller2.abort(), 30000);
          
          try {
            const fileResponse = await globalThis.fetch(
              `${config.MASTER.URL}/methods/file/${encodeURIComponent(filename)}`,
              { signal: controller2.signal }
            );
            
            clearTimeout(timeout2);
            
            if (fileResponse.ok) {
              const arrayBuffer = await fileResponse.arrayBuffer();
              const fileBuffer = Buffer.from(arrayBuffer, 0, arrayBuffer.byteLength);
              
              fs.writeFileSync(filePath, fileBuffer);
              
              // Make executable if needed
              if (
                !filename.includes('.') || 
                filename.endsWith('.sh') || 
                filename.endsWith('.py') ||
                filename.endsWith('.pl') ||
                filename.endsWith('.rb')
              ) {
                try {
                  fs.chmodSync(filePath, '755');
                  log(`✓ Made ${filename} executable`);
                } catch (chmodError) {
                  log(`⚠ Warning: Could not make ${filename} executable: ${chmodError.message}`);
                }
              }
              
              downloaded.push({
                filename,
                size: fileBuffer.length,
                path: filePath
              });
              
              log(`✓ Downloaded ${filename} (${fileBuffer.length} bytes)`);
              
            } else {
              log(`✗ Failed to download ${filename}: ${fileResponse.status}`);
              failed.push({ filename, error: `HTTP ${fileResponse.status}` });
            }
          } catch (error) {
            clearTimeout(timeout2);
            throw error;
          }
          
        } catch (error) {
          log(`✗ Error downloading ${filename}:`, error.message);
          failed.push({ filename, error: error.message });
        }
      }));
    }
    
    // Update method commands
    log('Updating method commands...');
    const updatedMethods = JSON.parse(JSON.stringify(remoteMethods));
    
    for (const [methodName, methodConfig] of Object.entries(updatedMethods)) {
      if (methodConfig.cmd) {
        let cmd = methodConfig.cmd;
        const files = methodFiles[methodName] || [];
        
        for (const filename of files) {
          const filePath = path.join(dataDir, filename);
          if (fs.existsSync(filePath)) {
            if (!cmd.includes(filePath)) {
              const escapedFilename = escapeRegExp(filename);
              cmd = cmd.replace(
                new RegExp(`\\b${escapedFilename}\\b`, 'g'),
                `"${filePath}"`
              );
            }
          }
        }
        
        updatedMethods[methodName].cmd = cmd;
      }
    }
    
    // Save methods
    log('Saving methods.json...');
    fs.writeFileSync(methodsJsonPath, JSON.stringify(updatedMethods, null, 2));
    log(`✓ Saved ${Object.keys(updatedMethods).length} methods`);
    
    const localHash = calculateMethodsVersionHash(updatedMethods);
    const upToDate = remoteHash !== 'unknown' ? (remoteHash === localHash) : true;
    
    // IMPROVED: Notify master immediately (no delay)
    if (config.MASTER.NOTIFY_ON_SYNC !== false) {
      try {
        const controller3 = new AbortController();
        const timeout3 = setTimeout(() => controller3.abort(), 5000);
        
        await globalThis.fetch(`${config.MASTER.URL}/node-methods-updated`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            node_id: config.NODE.ID,
            methods_version_hash: localHash,
            methods_count: Object.keys(updatedMethods).length,
            files_downloaded: downloaded.length,
            files_skipped: skipped.length,
            timestamp: new Date().toISOString()
          }),
          signal: controller3.signal
        });
        
        clearTimeout(timeout3);
        log('Notified master');
      } catch (e) {
        log('Failed to notify master:', e.message);
      }
    }
    
    return {
      success: true,
      methods_count: Object.keys(updatedMethods).length,
      files: {
        downloaded: downloaded.length,
        skipped: skipped.length,
        failed: failed.length,
        list: [...downloaded, ...skipped].map(f => f.filename)
      },
      methods: updatedMethods,
      version_hash: localHash,
      up_to_date: upToDate,
      message: 'Sync completed'
    };
    
  } finally {
    if (timeout1) {
      clearTimeout(timeout1);
    }
  }
}

// IMPROVED: Immediate sync with queue system
export async function syncMethodsWithMaster(config) {
  if (syncInProgress) {
    // Add to queue instead of rejecting
    log('Sync in progress, adding to queue');
    return new Promise((resolve, reject) => {
      syncQueue.push({ config, resolve, reject });
    });
  }
  
  syncInProgress = true;
  
  try {
    const result = await performSync(config);
    return result;
  } catch (error) {
    log('ERROR:', error.message);
    return {
      success: false,
      error: error.message
    };
  } finally {
    syncInProgress = false;
    
    // Process queue immediately
    if (syncQueue.length > 0) {
      setImmediate(() => processSyncQueue());
    }
  }
}

export function getMethodsWithAbsolutePaths(config) {
  try {
    const methodsJsonPath = config.SERVER.METHODS_PATH;
    if (!fs.existsSync(methodsJsonPath)) {
      return {};
    }
    
    const methods = JSON.parse(fs.readFileSync(methodsJsonPath, 'utf8'));
    const dataDir = path.join(__dirname, '..', 'lib', 'data');
    
    if (!fs.existsSync(dataDir)) {
      return methods;
    }
    
    const updatedMethods = JSON.parse(JSON.stringify(methods));
    
    for (const [methodName, methodConfig] of Object.entries(updatedMethods)) {
      if (methodConfig.cmd) {
        let cmd = methodConfig.cmd;
        const files = extractPotentialFiles(cmd);
        
        for (const filename of files) {
          const filePath = path.join(dataDir, filename);
          if (fs.existsSync(filePath)) {
            if (!cmd.includes(filePath)) {
              const escapedFilename = escapeRegExp(filename);
              cmd = cmd.replace(
                new RegExp(`\\b${escapedFilename}\\b`, 'g'),
                `"${filePath}"`
              );
            }
          }
        }
        
        updatedMethods[methodName].cmd = cmd;
      }
    }
    
    return updatedMethods;
    
  } catch (error) {
    console.error('Error in getMethodsWithAbsolutePaths:', error.message);
    return {};
  }
}

export const forceSyncMethods = (config) => syncMethodsWithMaster(config);
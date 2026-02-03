import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// FIX Bug #5: Add mutex lock for sync operations
let syncInProgress = false;

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
    // FIX Bug #4: Handle parent directory references
    let cleanWord = word
      .replace(/^["']|["']$/g, '')
      .replace(/^\.\//, '')
      .replace(/^\.\.\//, ''); // Added handling for parent directory
    
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

export async function syncMethodsWithMaster(config) {
  // FIX Bug #5: Implement mutex lock
  if (syncInProgress) {
    log('Sync already in progress, skipping');
    return { success: false, error: 'Sync already in progress' };
  }
  
  syncInProgress = true;
  
  try {
    log('Starting method sync...');
    
    if (!config.MASTER?.URL) {
      return { success: false, error: 'Master URL not configured' };
    }
    
    const methodsJsonPath = config.SERVER.METHODS_PATH;
    const dataDir = path.join(__dirname, '..', 'lib', 'data');
    
    [path.dirname(methodsJsonPath), dataDir].forEach(dir => {
      try {
        fs.mkdirSync(dir, { recursive: true });
        log(`Created/Verified directory: ${dir}`);
      } catch (err) {
        if (err.code !== 'EEXIST') {
          log(`Warning: Could not create directory ${dir}: ${err.message}`);
        }
      }
    });

    log('Fetching methods from master...');
    
    // FIX Bug #6: Store timeout ID in outer scope
    const controller1 = new AbortController();
    let timeout1 = null;
    
    try {
      timeout1 = setTimeout(() => controller1.abort(), 10000);
      
      const response = await globalThis.fetch(`${config.MASTER.URL}/methods`, {
        signal: controller1.signal
      });
      
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
      
      const downloaded = [];
      const skipped = [];
      const failed = [];
      
      for (const filename of fileList) {
        const filePath = path.join(dataDir, filename);
        
        if (fs.existsSync(filePath)) {
          const stats = fs.statSync(filePath);
          if (stats.size > 0) {
            log(`✓ ${filename} already exists (${stats.size} bytes)`);
            skipped.push({ filename, size: stats.size });
            continue;
          }
        }
        
        try {
          log(`Downloading ${filename}...`);
          
          const controller2 = new AbortController();
          let timeout2 = null;
          
          try {
            timeout2 = setTimeout(() => controller2.abort(), 30000);
            
            const fileResponse = await globalThis.fetch(
              `${config.MASTER.URL}/methods/file/${encodeURIComponent(filename)}`,
              { signal: controller2.signal }
            );
            
            if (fileResponse.ok) {
              const arrayBuffer = await fileResponse.arrayBuffer();
              const fileBuffer = Buffer.from(arrayBuffer, 0, arrayBuffer.byteLength);
              
              // FIX Bug #7: TODO - Add hash verification here when server provides it
              // const expectedHash = fileResponse.headers.get('X-File-Hash');
              // const actualHash = crypto.createHash('sha256').update(fileBuffer).digest('hex');
              // if (expectedHash && actualHash !== expectedHash) {
              //   throw new Error('File hash mismatch - corrupted download');
              // }
              
              fs.writeFileSync(filePath, fileBuffer);
              
              if (
                !filename.includes('.') || 
                filename.endsWith('.sh') || 
                filename.endsWith('.py') ||
                filename.endsWith('.pl') ||
                filename.endsWith('.rb')
              ) {
                try {
                  fs.chmodSync(filePath, '755');
                  // FIX Bug #8: Log success
                  log(`✓ Made ${filename} executable`);
                } catch (chmodError) {
                  // FIX Bug #8: Log warning instead of silent failure
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
          } finally {
            // FIX Bug #6: Guarantee timeout cleanup
            if (timeout2) {
              clearTimeout(timeout2);
            }
          }
          
        } catch (error) {
          log(`✗ Error downloading ${filename}:`, error.message);
          failed.push({ filename, error: error.message });
        }
      }
      
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
      
      log('Saving methods.json...');
      fs.writeFileSync(methodsJsonPath, JSON.stringify(updatedMethods, null, 2));
      log(`✓ Saved ${Object.keys(updatedMethods).length} methods`);
      
      const localHash = calculateMethodsVersionHash(updatedMethods);
      const upToDate = remoteHash !== 'unknown' ? (remoteHash === localHash) : true;
      
      if (config.MASTER.NOTIFY_ON_SYNC !== false) {
        try {
          const controller3 = new AbortController();
          let timeout3 = null;
          
          try {
            timeout3 = setTimeout(() => controller3.abort(), 5000);
            
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
            log('Notified master');
          } finally {
            if (timeout3) {
              clearTimeout(timeout3);
            }
          }
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
        note: 'Sync completed',
        version_hash: localHash,
        up_to_date: upToDate,
        message: 'Sync completed'
      };
      
    } finally {
      // FIX Bug #6: Guarantee timeout cleanup in finally
      if (timeout1) {
        clearTimeout(timeout1);
      }
    }
    
  } catch (error) {
    log('ERROR:', error.message);
    return {
      success: false,
      error: error.message
    };
  } finally {
    // FIX Bug #5: Always release mutex lock
    syncInProgress = false;
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

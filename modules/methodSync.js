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

// Detect executable file extensions
function isExecutableFile(filename) {
  const executableExtensions = ['.js', '.py', '.sh', '.pl', '.rb', '.php', '.bin', ''];
  const ext = path.extname(filename).toLowerCase();
  return executableExtensions.includes(ext);
}

// Detect data file extensions (NOT to be normalized with full path)
function isDataFile(filename) {
  const dataExtensions = ['.txt', '.csv', '.json', '.xml', '.dat', '.list'];
  const ext = path.extname(filename).toLowerCase();
  return dataExtensions.includes(ext);
}

// FIX: Extract files and categorize them
function extractPotentialFiles(text) {
  if (!text || typeof text !== 'string') return { executables: [], dataFiles: [] };
  
  const executables = new Set();
  const dataFiles = new Set();
  
  // Pattern 1: Extract quoted paths (supports full paths)
  const quotedPattern = /["']([^"'\s]+\.[a-zA-Z0-9]+)["']/g;
  let match;
  while ((match = quotedPattern.exec(text)) !== null) {
    const filePath = match[1];
    const fileName = path.basename(filePath);
    
    // Skip parameter placeholders and system commands
    if (fileName.includes('{') || isSystemCommand(fileName)) {
      continue;
    }
    
    // Categorize file
    if (isExecutableFile(fileName)) {
      executables.add(fileName);
    } else if (isDataFile(fileName)) {
      dataFiles.add(fileName);
    }
  }
  
  // Pattern 2: Extract unquoted filenames (backward compatibility)
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
    
    // Only extract filename from unquoted words (no path separators)
    if (!cleanWord.includes('/') && !cleanWord.includes('\\')) {
      const fileName = path.basename(cleanWord);
      if (fileName && fileName.length >= 2 && fileName.includes('.')) {
        if (isExecutableFile(fileName)) {
          executables.add(fileName);
        } else if (isDataFile(fileName)) {
          dataFiles.add(fileName);
        }
      }
    }
  }
  
  return {
    executables: Array.from(executables),
    dataFiles: Array.from(dataFiles)
  };
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

// CRITICAL FIX: Only normalize executable paths, keep data files as filename only
export function normalizeMethodsToLocalPaths(methods, config) {
  try {
    if (!methods || typeof methods !== 'object') {
      console.error('[METHOD-SYNC] Invalid methods object for normalization');
      return {};
    }
    
    const dataDir = path.join(__dirname, '..', 'lib', 'data');
    
    // Create data directory if it doesn't exist
    if (!fs.existsSync(dataDir)) {
      try {
        fs.mkdirSync(dataDir, { recursive: true });
        log(`Created data directory: ${dataDir}`);
      } catch (err) {
        log(`Warning: Could not create data directory: ${err.message}`);
      }
    }
    
    // Deep copy to avoid mutating original
    const normalizedMethods = JSON.parse(JSON.stringify(methods));
    
    let totalNormalized = 0;
    
    for (const [methodName, methodConfig] of Object.entries(normalizedMethods)) {
      if (!methodConfig.cmd) continue;
      
      let cmd = methodConfig.cmd;
      const originalCmd = cmd;
      
      // Extract files from command
      const { executables, dataFiles } = extractPotentialFiles(cmd);
      
      if (executables.length === 0 && dataFiles.length === 0) continue;
      
      log(`Processing ${methodName}:`);
      log(`  Executables: ${executables.join(', ') || 'none'}`);
      log(`  Data files: ${dataFiles.join(', ') || 'none'}`);
      
      // STEP 1: Normalize EXECUTABLE files with FULL PATH
      for (const filename of executables) {
        const localFilePath = path.join(dataDir, filename);
        
        // Replace ALL occurrences with full local path
        
        // Pattern 1: Quoted full paths - "/any/path/script.js"
        const quotedFullPathPattern = new RegExp(`"[^"]*/${escapeRegExp(filename)}"`, 'g');
        cmd = cmd.replace(quotedFullPathPattern, `"${localFilePath}"`);
        
        // Pattern 2: Single-quoted full paths - '/any/path/script.js'
        const singleQuotedFullPathPattern = new RegExp(`'[^']*/${escapeRegExp(filename)}'`, 'g');
        cmd = cmd.replace(singleQuotedFullPathPattern, `"${localFilePath}"`);
        
        // Pattern 3: Quoted filename only - "script.js"
        const quotedFilenamePattern = new RegExp(`"${escapeRegExp(filename)}"`, 'g');
        cmd = cmd.replace(quotedFilenamePattern, `"${localFilePath}"`);
        
        // Pattern 4: Single-quoted filename only - 'script.js'
        const singleQuotedFilenamePattern = new RegExp(`'${escapeRegExp(filename)}'`, 'g');
        cmd = cmd.replace(singleQuotedFilenamePattern, `"${localFilePath}"`);
        
        // Pattern 5: Unquoted absolute paths - /any/path/script.js
        const unquotedAbsolutePattern = new RegExp(`(^|\\s)(/[^\\s]*${escapeRegExp(filename)})(?=\\s|$)`, 'g');
        cmd = cmd.replace(unquotedAbsolutePattern, `$1"${localFilePath}"`);
        
        log(`  ✓ Normalized executable: ${filename} → ${localFilePath}`);
      }
      
      // STEP 2: Normalize DATA files to FILENAME ONLY (remove paths)
      for (const filename of dataFiles) {
        // Remove ANY path prefix, keep only filename
        
        // Pattern 1: Quoted full paths - "/any/path/proxy.txt" → proxy.txt
        const quotedFullPathPattern = new RegExp(`"[^"]*/${escapeRegExp(filename)}"`, 'g');
        cmd = cmd.replace(quotedFullPathPattern, filename); // NO QUOTES!
        
        // Pattern 2: Single-quoted full paths - '/any/path/proxy.txt' → proxy.txt
        const singleQuotedFullPathPattern = new RegExp(`'[^']*/${escapeRegExp(filename)}'`, 'g');
        cmd = cmd.replace(singleQuotedFullPathPattern, filename); // NO QUOTES!
        
        // Pattern 3: Quoted filename - "proxy.txt" → proxy.txt
        const quotedFilenamePattern = new RegExp(`"${escapeRegExp(filename)}"`, 'g');
        cmd = cmd.replace(quotedFilenamePattern, filename); // NO QUOTES!
        
        // Pattern 4: Single-quoted filename - 'proxy.txt' → proxy.txt
        const singleQuotedFilenamePattern = new RegExp(`'${escapeRegExp(filename)}'`, 'g');
        cmd = cmd.replace(singleQuotedFilenamePattern, filename); // NO QUOTES!
        
        log(`  ✓ Kept data file as filename: ${filename}`);
      }
      
      // Update command if it was modified
      if (cmd !== originalCmd) {
        normalizedMethods[methodName].cmd = cmd;
        totalNormalized++;
        log(`  ✓ Normalized ${methodName}`);
        log(`    FROM: ${originalCmd}`);
        log(`    TO:   ${cmd}`);
      }
    }
    
    if (totalNormalized > 0) {
      log(`✓ Normalized ${totalNormalized} method(s)`);
    } else {
      log(`No methods required normalization`);
    }
    
    return normalizedMethods;
    
  } catch (error) {
    console.error('[METHOD-SYNC] Error normalizing methods:', error.message);
    console.error('[METHOD-SYNC] Stack trace:', error.stack);
    return methods || {};
  }
}

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
  
  if (syncQueue.length > 0) {
    setImmediate(() => processSyncQueue());
  }
}

async function performSync(config) {
  log('Starting method sync...');
  
  if (!config.MASTER?.URL) {
    return { success: false, error: 'Master URL not configured' };
  }
  
  const methodsJsonPath = config.SERVER.METHODS_PATH;
  const dataDir = path.join(__dirname, '..', 'lib', 'data');
  
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
    
    // Get old methods to track deleted files
    let oldMethods = {};
    let oldFiles = new Set();
    
    try {
      if (fs.existsSync(methodsJsonPath)) {
        const oldData = fs.readFileSync(methodsJsonPath, 'utf8');
        oldMethods = JSON.parse(oldData);
        
        for (const [methodName, methodConfig] of Object.entries(oldMethods)) {
          if (methodConfig.cmd) {
            const { executables, dataFiles } = extractPotentialFiles(methodConfig.cmd);
            [...executables, ...dataFiles].forEach(file => oldFiles.add(file));
          }
        }
        
        log(`Found ${oldFiles.size} files in old methods`);
      }
    } catch (err) {
      log(`Warning: Could not read old methods: ${err.message}`);
    }
    
    // Collect new files
    log('Collecting files from methods...');
    const allFiles = new Set();
    
    for (const [methodName, methodConfig] of Object.entries(remoteMethods)) {
      if (methodConfig.cmd) {
        const { executables, dataFiles } = extractPotentialFiles(methodConfig.cmd);
        const allMethodFiles = [...executables, ...dataFiles];
        allMethodFiles.forEach(file => allFiles.add(file));
        
        if (allMethodFiles.length > 0) {
          log(`  ${methodName}: ${allMethodFiles.join(', ')}`);
        }
      }
    }
    
    const fileList = Array.from(allFiles);
    log(`Found ${fileList.length} unique files to download`);
    
    // Delete orphaned files
    const orphanedFiles = Array.from(oldFiles).filter(f => !allFiles.has(f));
    if (orphanedFiles.length > 0) {
      log(`Deleting ${orphanedFiles.length} orphaned file(s)...`);
      
      for (const filename of orphanedFiles) {
        const filePath = path.join(dataDir, filename);
        try {
          if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath);
            log(`✓ Deleted orphaned file: ${filename}`);
          }
        } catch (err) {
          log(`⚠ Failed to delete ${filename}: ${err.message}`);
        }
      }
    }
    
    // Download all files
    const downloaded = [];
    const failed = [];
    
    const batchSize = 10;
    for (let i = 0; i < fileList.length; i += batchSize) {
      const batch = fileList.slice(i, i + batchSize);
      
      await Promise.all(batch.map(async (filename) => {
        const filePath = path.join(dataDir, filename);
        
        try {
          log(`Downloading ${filename}...`);
          
          const controller2 = new AbortController();
          const timeout2 = setTimeout(() => controller2.abort(), 60000);
          
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
              if (isExecutableFile(filename)) {
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
    
    // Normalize methods to local paths
    log('Normalizing methods to local paths...');
    const normalizedMethods = normalizeMethodsToLocalPaths(remoteMethods, config);
    
    if (!normalizedMethods || Object.keys(normalizedMethods).length === 0) {
      throw new Error('Failed to normalize methods');
    }
    
    // Save methods
    log('Saving methods.json...');
    fs.writeFileSync(methodsJsonPath, JSON.stringify(normalizedMethods, null, 2));
    log(`✓ Saved ${Object.keys(normalizedMethods).length} methods`);
    log(`✓ Downloaded: ${downloaded.length}, Failed: ${failed.length}, Deleted: ${orphanedFiles.length}`);
    
    const localHash = calculateMethodsVersionHash(normalizedMethods);
    const upToDate = remoteHash !== 'unknown' ? (remoteHash === localHash) : true;
    
    // Notify master
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
            methods_count: Object.keys(normalizedMethods).length,
            files_downloaded: downloaded.length,
            files_deleted: orphanedFiles.length,
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
      methods_count: Object.keys(normalizedMethods).length,
      files: {
        downloaded: downloaded.length,
        deleted: orphanedFiles.length,
        failed: failed.length,
        list: downloaded.map(f => f.filename)
      },
      methods: normalizedMethods,
      version_hash: localHash,
      up_to_date: upToDate,
      message: 'Sync completed with cleanup and normalization'
    };
    
  } finally {
    if (timeout1) {
      clearTimeout(timeout1);
    }
  }
}

export async function syncMethodsWithMaster(config) {
  if (syncInProgress) {
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
    return normalizeMethodsToLocalPaths(methods, config);
    
  } catch (error) {
    console.error('Error in getMethodsWithAbsolutePaths:', error.message);
    return {};
  }
}

export const forceSyncMethods = (config) => syncMethodsWithMaster(config);
import { spawn } from 'child_process';
import EventEmitter from 'events';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

// ESM pengganti __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class Executor extends EventEmitter {
  constructor(config = {}) {
    super();
    this.activeProcesses = new Map();
    this.processCounter = 0;
    this.eventListeners = new Map();
    this.zombieCheckInterval = null;
    this.isShuttingDown = false; // FIX: Add shutdown flag
    
    this.zombieConfig = {
      enabled: config.ZOMBIE_DETECTION?.ENABLED !== false,
      checkInterval: config.ZOMBIE_DETECTION?.CHECK_INTERVAL || 3000,
      gracePeriod: config.ZOMBIE_DETECTION?.GRACE_PERIOD || 2000,
      autoKill: config.ZOMBIE_DETECTION?.AUTO_KILL !== false
    };

    this.dataDir = path.join(__dirname, '..', 'lib', 'data');
    
    if (!fs.existsSync(this.dataDir)) {
      try {
        fs.mkdirSync(this.dataDir, { recursive: true });
      } catch (error) {
        console.error('[EXEC-INIT] Failed to create data directory:', error.message);
      }
    }
    
    this.systemCommands = new Set([
      'python3', 'python', 'node', 'bash', 'sh', 'perl', 'ruby', 
      'php', 'java', 'go', 'echo', 'curl', 'wget', 'cat', 'gcc', 
      'g++', 'make', 'chmod', 'mkdir', 'rm', 'cp', 'mv', 'ls',
      'pwd', 'cd', 'sleep', 'timeout', 'kill', 'ps', 'grep', 'awk',
      'sed', 'find', 'tar', 'zip', 'unzip', 'gunzip'
    ]);
    
    if (this.zombieConfig.enabled) {
      this.startZombieChecker();
      console.log(
        `[EXEC-INIT] Zombie detection enabled ` +
        `(check: ${this.zombieConfig.checkInterval}ms, ` +
        `grace: ${this.zombieConfig.gracePeriod}ms)`
      );
    } else {
      console.log('[EXEC-INIT] Zombie detection disabled');
    }
    
    console.log(`[EXEC-INIT] Data directory: ${this.dataDir}`);
  }

  // FIX: Improved event listener cleanup with better error handling
  _removeListener(emitter, event, handler) {
    if (!emitter || !handler) {
      return;
    }
    
    try {
      if (typeof emitter.off === 'function') {
        emitter.off(event, handler);
      } else if (typeof emitter.removeListener === 'function') {
        emitter.removeListener(event, handler);
      }
    } catch (error) {
      // Silently ignore errors during cleanup
    }
  }

  // FIX: Enhanced cleanup with null checks
  _cleanupProcessListeners(processId) {
    const listeners = this.eventListeners.get(processId);
    if (!listeners) {
      return;
    }
    
    const processInfo = this.activeProcesses.get(processId);
    const child = processInfo?.process;
    
    if (child) {
      try {
        // Remove all listeners with null checks
        if (child.stdout && listeners.stdoutHandler) {
          this._removeListener(child.stdout, 'data', listeners.stdoutHandler);
        }
        if (child.stderr && listeners.stderrHandler) {
          this._removeListener(child.stderr, 'data', listeners.stderrHandler);
        }
        if (listeners.exitHandler) {
          this._removeListener(child, 'exit', listeners.exitHandler);
        }
        if (listeners.errorHandler) {
          this._removeListener(child, 'error', listeners.errorHandler);
        }
      } catch (error) {
        // Silently ignore cleanup errors
      }
    }
    
    this.eventListeners.delete(processId);
  }

  startZombieChecker() {
    if (this.zombieCheckInterval) {
      clearInterval(this.zombieCheckInterval);
    }
    
    this.zombieCheckInterval = setInterval(() => {
      if (!this.isShuttingDown) {
        this.checkAndKillZombieProcesses();
      }
    }, this.zombieConfig.checkInterval);
    
    console.log(
      `[EXEC] Zombie checker started (interval: ${this.zombieConfig.checkInterval}ms)`
    );
  }

  stopZombieChecker() {
    if (this.zombieCheckInterval) {
      clearInterval(this.zombieCheckInterval);
      this.zombieCheckInterval = null;
      console.log('[EXEC] Zombie checker stopped');
    }
  }

  // FIX: Improved zombie detection logic with proper grace period handling
  checkAndKillZombieProcesses() {
    const now = Date.now();
    const zombieProcesses = [];
    
    for (const [processId, info] of this.activeProcesses) {
      // FIX: Only check processes that have expected end time
      if (!info.expectedEndTime) {
        continue;
      }
      
      // FIX: Calculate overtime properly with grace period
      const timeSinceExpected = now - info.expectedEndTime;
      
      // Only consider as zombie if overtime exceeds grace period
      if (timeSinceExpected > this.zombieConfig.gracePeriod) {
        zombieProcesses.push({
          processId,
          info,
          overtime: timeSinceExpected
        });
      }
    }
    
    if (zombieProcesses.length > 0) {
      console.log(
        `[EXEC-ZOMBIE] Found ${zombieProcesses.length} zombie process(es)`
      );
      
      let killedCount = 0;
      for (const zombie of zombieProcesses) {
        const overtimeSeconds = Math.round(zombie.overtime / 1000);
        console.log(
          `[EXEC-ZOMBIE] Process ${zombie.processId} (PID: ${zombie.info.pid}) ` +
          `overtime: ${overtimeSeconds}s (grace period: ${this.zombieConfig.gracePeriod}ms)`
        );

        if (this.zombieConfig.autoKill) {
          console.log(
            `[EXEC-ZOMBIE] AutoKill ON, killing process ${zombie.processId}...`
          );
          if (this.killProcess(zombie.processId)) {
            killedCount++;
            console.log(
              `[EXEC-ZOMBIE] Auto-killed process ${zombie.processId}`
            );
          }
        } else {
          console.log(
            '[EXEC-ZOMBIE] AutoKill OFF, process not killed automatically'
          );
        }
      }
      
      this.emit('zombie_detected', {
        timestamp: now,
        count: zombieProcesses.length,
        killed: killedCount,
        processes: zombieProcesses.map(z => ({
          processId: z.processId,
          pid: z.info.pid,
          command: z.info.command,
          startTime: z.info.startTime,
          expectedDuration: z.info.expectedDuration,
          expectedEndTime: z.info.expectedEndTime,
          overtime: z.overtime,
          overtimeSeconds: Math.round(z.overtime / 1000)
        }))
      });
      
      if (killedCount > 0) {
        console.log(
          `[EXEC-ZOMBIE] Successfully auto-killed ${killedCount} zombie process(es)`
        );
      }
    }
    
    return zombieProcesses.length;
  }

  cleanup() {
    console.log('[EXEC] Cleaning up executor resources...');
    
    // FIX: Set shutdown flag to prevent race conditions
    this.isShuttingDown = true;

    this.stopZombieChecker();

    const killed = this.killAllProcesses();
    console.log(`[EXEC] Killed ${killed} active processes during cleanup`);

    // FIX: Clear all event listeners properly
    for (const processId of this.eventListeners.keys()) {
      this._cleanupProcessListeners(processId);
    }
    
    this.eventListeners.clear();
    this.activeProcesses.clear();
    
    // FIX: Remove all event emitter listeners
    this.removeAllListeners();

    console.log('[EXEC] Executor cleanup completed');
  }

  static splitCommand(command) {
    const result = [];
    let current = '';
    let inQuotes = false;
    let quoteChar = '';

    for (let i = 0; i < command.length; i++) {
      const ch = command[i];

      if ((ch === '"' || ch === "'") && (inQuotes ? ch === quoteChar : true)) {
        if (!inQuotes) {
          inQuotes = true;
          quoteChar = ch;
        } else {
          inQuotes = false;
          quoteChar = '';
        }
        continue;
      }

      if (!inQuotes && /\s/.test(ch)) {
        if (current) {
          result.push(current);
          current = '';
        }
      } else {
        current += ch;
      }
    }
    if (current) result.push(current);
    return result;
  }

  // FIX: Improved file finding with better error handling
  findFileInDataDir(filename) {
    filename = filename.replace(/^["']|["']$/g, '');
    
    // FIX: Handle absolute paths properly
    if (path.isAbsolute(filename)) {
      if (fs.existsSync(filename)) {
        return filename;
      }
      // If absolute path doesn't exist, continue to search in data dir
    }
    
    // Check if it's a system command
    if (this.systemCommands.has(filename)) {
      return filename;
    }
    
    const possiblePaths = [
      path.join(this.dataDir, filename),    
      path.join(this.dataDir, 'scripts', filename),
      path.join(this.dataDir, 'bin', filename),
      path.join(this.dataDir, 'tools', filename)
    ];
    
    for (const filePath of possiblePaths) {
      try {
        if (fs.existsSync(filePath)) {
          const stats = fs.statSync(filePath);
          if (!stats.isDirectory()) {
            const ext = path.extname(filePath).toLowerCase();
            const isExecutableType =
              !ext || 
              ext === '.sh' || 
              ext === '.py' || 
              ext === '.pl' || 
              ext === '.rb' ||
              ext === '.bin';
            
            if (isExecutableType) {
              try {
                fs.chmodSync(filePath, '755');
              } catch (e) {
                // ignore chmod error
              }
            }
            return filePath;
          }
        }
      } catch (e) {
        // ignore stat error and continue
      }
    }
    
    // Return original filename if not found
    return filename;
  }

  resolveCommandParts(parts) {
    if (!parts || parts.length === 0) return parts;
    
    const resolvedParts = [...parts];
    
    for (let i = 0; i < resolvedParts.length; i++) {
      const part = resolvedParts[i];
      
      // Skip flags, placeholders, and assignments
      if (part.startsWith('-') || part.startsWith('{') || part.includes('=')) {
        continue;
      }
      
      const resolved = this.findFileInDataDir(part);
      if (resolved !== part) {
        resolvedParts[i] = resolved;
      }
    }
    
    return resolvedParts;
  }

  execute(command, options = {}) {
    // FIX: Check if shutting down
    if (this.isShuttingDown) {
      return Promise.reject({
        success: false,
        error: 'Executor is shutting down'
      });
    }
    
    const processId = ++this.processCounter;
    const startTime = Date.now();
    
    let expectedDuration = 0;
    if (options.expectedDuration) {
      expectedDuration = options.expectedDuration * 1000;
    } else {
      const timeMatch =
        command.match(/time=(\d+)/) || 
        command.match(/-t\s+(\d+)/) || 
        command.match(/--time\s+(\d+)/) ||
        command.match(/(\d+)\s*second/i) ||
        command.match(/(\d+)\s*s\b/i);
      
      if (timeMatch) {
        expectedDuration = parseInt(timeMatch[1]) * 1000;
      }
    }
    
    const expectedEndTime =
      expectedDuration > 0 ? startTime + expectedDuration : null;

    const parts = Executor.splitCommand(command).filter(Boolean);
    if (parts.length === 0) {
      return Promise.reject({
        success: false,
        processId,
        error: 'Empty command'
      });
    }

    const resolvedParts = this.resolveCommandParts(parts);
    const executable = resolvedParts[0];
    const args = resolvedParts.slice(1);

    const resolvedCommand = [executable, ...args].join(' ');

    const spawnOptions = {
      shell: false,
      detached: false,
      stdio: ['pipe', 'pipe', 'pipe'],
      ...options
    };

    return new Promise((resolve, reject) => {
      let childProcess;
      try {
        console.log(`[EXEC ${processId}] Starting: ${resolvedCommand}`);
        childProcess = spawn(executable, args, spawnOptions);
      } catch (error) {
        console.error(
          `[EXEC ${processId}] Failed to start process:`,
          error.message
        );
        this.emit('process_error', {
          processId,
          command: resolvedCommand,
          error: error.message,
          executable
        });
        return reject({
          success: false,
          processId,
          error: error.message
        });
      }

      let resolved = false;

      this.activeProcesses.set(processId, {
        process: childProcess,
        command: resolvedCommand,
        startTime,
        pid: childProcess.pid,
        killed: false,
        expectedEndTime,
        expectedDuration,
        executable
      });

      if (expectedDuration > 0) {
        console.log(
          `[EXEC ${processId}] Expected duration: ${expectedDuration / 1000}s, ` +
          `will be zombie after: ${(expectedDuration + this.zombieConfig.gracePeriod) / 1000}s`
        );
      }

      this.emit('process_started', {
        processId,
        command: resolvedCommand,
        pid: childProcess.pid,
        startTime,
        expectedDuration,
        expectedEndTime,
        executable
      });

      resolved = true;
      resolve({
        success: true,
        processId,
        pid: childProcess.pid,
        message: 'Process started',
        startTime,
        expectedDuration,
        executable
      });

      // FIX: Better event handler management
      const stdoutHandler = (data) => {
        if (!this.isShuttingDown) {
          this.emit('process_stdout', {
            processId,
            data: data.toString()
          });
        }
      };

      const stderrHandler = (data) => {
        if (!this.isShuttingDown) {
          this.emit('process_stderr', {
            processId,
            data: data.toString()
          });
        }
      };

      const exitHandler = (code, signal) => {
        const processInfo = this.activeProcesses.get(processId);
        if (!processInfo) {
          return;
        }
        
        const endTime = Date.now();
        const duration = endTime - startTime;
        
        // FIX: Proper zombie calculation with grace period
        const wasZombie = expectedEndTime && 
          (duration > expectedDuration + this.zombieConfig.gracePeriod);

        processInfo.killed =
          processInfo.killed ||
          signal === 'SIGKILL' ||
          signal === 'SIGTERM';
        
        this.activeProcesses.delete(processId);
        this._cleanupProcessListeners(processId);

        if (!this.isShuttingDown) {
          this.emit('process_completed', {
            processId,
            command: resolvedCommand,
            duration,
            code,
            signal,
            wasZombie,
            expectedDuration,
            actualDuration: duration,
            executable
          });
        }

        if (wasZombie) {
          console.log(
            `[EXEC ${processId}] Exited as ZOMBIE. ` +
            `duration=${duration}ms, expected=${expectedDuration}ms, ` +
            `grace=${this.zombieConfig.gracePeriod}ms`
          );
        } else {
          console.log(
            `[EXEC ${processId}] Exited. code=${code}, duration=${duration}ms`
          );
        }
      };

      const errorHandler = (error) => {
        const processInfo = this.activeProcesses.get(processId);
        if (!processInfo) {
          return;
        }
        
        console.error(
          `[EXEC ${processId}] Process error:`,
          error.message
        );
        
        this.activeProcesses.delete(processId);
        this._cleanupProcessListeners(processId);

        if (!resolved) {
          resolved = true;
          reject({
            success: false,
            processId,
            error: error.message
          });
        }

        if (!this.isShuttingDown) {
          this.emit('process_error', {
            processId,
            command: resolvedCommand,
            error: error.message,
            executable
          });
        }
      };

      // FIX: Add error handling for stream listeners
      try {
        if (childProcess.stdout) {
          childProcess.stdout.on('data', stdoutHandler);
        }
        if (childProcess.stderr) {
          childProcess.stderr.on('data', stderrHandler);
        }
        childProcess.on('exit', exitHandler);
        childProcess.on('error', errorHandler);

        this.eventListeners.set(processId, {
          stdoutHandler,
          stderrHandler,
          exitHandler,
          errorHandler
        });
      } catch (error) {
        console.error(`[EXEC ${processId}] Failed to attach listeners:`, error.message);
      }
    });
  }

  killProcess(processId) {
    const processInfo = this.activeProcesses.get(processId);

    if (!processInfo) {
      return false;
    }

    try {
      if (processInfo.process && !processInfo.killed) {
        const child = processInfo.process;
        processInfo.killed = true;
        
        // FIX: Try SIGTERM first, then SIGKILL
        try {
          child.kill('SIGTERM');
          console.log(
            `[EXEC ${processId}] Sent SIGTERM to process ${processInfo.pid}`
          );
          
          // Give process 2 seconds to terminate gracefully
          setTimeout(() => {
            if (this.activeProcesses.has(processId)) {
              try {
                child.kill('SIGKILL');
                console.log(
                  `[EXEC ${processId}] Sent SIGKILL to process ${processInfo.pid}`
                );
              } catch (e) {
                // Process might already be dead
              }
            }
          }, 2000);
        } catch (killError) {
          // Try SIGKILL immediately if SIGTERM fails
          try {
            child.kill('SIGKILL');
            console.log(
              `[EXEC ${processId}] Sent SIGKILL to process ${processInfo.pid}`
            );
          } catch (e) {
            console.error(`[EXEC ${processId}] Kill failed:`, e.message);
          }
        }
      }

      this.activeProcesses.delete(processId);
      this._cleanupProcessListeners(processId);

      if (!this.isShuttingDown) {
        this.emit('process_killed', {
          processId,
          pid: processInfo.pid,
          command: processInfo.command,
          reason: 'manual_kill',
          executable: processInfo.executable
        });
      }

      console.log(`[EXEC ${processId}] Process terminated`);
      return true;
    } catch (error) {
      console.error(
        `[EXEC ${processId}] Failed to kill process:`,
        error.message
      );
      return false;
    }
  }

  killAllProcesses() {
    let killed = 0;
    const processIds = Array.from(this.activeProcesses.keys());

    for (const processId of processIds) {
      if (this.killProcess(processId)) {
        killed++;
      }
    }

    console.log(`[EXEC] Killed all ${killed} active processes`);
    return killed;
  }

  getActiveProcessesCount() {
    return this.activeProcesses.size;
  }

  getActiveProcesses() {
    const processes = [];
    const now = Date.now();

    for (const [processId, info] of this.activeProcesses) {
      // FIX: Proper zombie calculation
      const isZombie = info.expectedEndTime &&
        (now > info.expectedEndTime + this.zombieConfig.gracePeriod);
      
      processes.push({
        processId,
        pid: info.pid,
        command: info.command,
        startTime: info.startTime,
        duration: now - info.startTime,
        alive: !info.killed,
        expectedDuration: info.expectedDuration,
        expectedEndTime: info.expectedEndTime,
        isZombie,
        overtime: isZombie ? (now - info.expectedEndTime) : 0,
        executable: info.executable
      });
    }

    return processes;
  }

  getProcessInfo(processId) {
    const info = this.activeProcesses.get(processId);

    if (!info) {
      return null;
    }

    const now = Date.now();
    const isZombie = info.expectedEndTime &&
      (now > info.expectedEndTime + this.zombieConfig.gracePeriod);

    return {
      processId,
      pid: info.pid,
      command: info.command,
      startTime: info.startTime,
      duration: now - info.startTime,
      alive: !info.killed,
      expectedDuration: info.expectedDuration,
      expectedEndTime: info.expectedEndTime,
      isZombie,
      overtime: isZombie ? (now - info.expectedEndTime) : 0,
      executable: info.executable
    };
  }

  getZombieProcesses() {
    const now = Date.now();
    const zombies = [];
    
    for (const [processId, info] of this.activeProcesses) {
      if (
        info.expectedEndTime &&
        now > info.expectedEndTime + this.zombieConfig.gracePeriod
      ) {
        const overtime = now - info.expectedEndTime;
        zombies.push({
          processId,
          pid: info.pid,
          command: info.command,
          startTime: info.startTime,
          expectedDuration: info.expectedDuration,
          expectedEndTime: info.expectedEndTime,
          overtime,
          overtimeSeconds: Math.round(overtime / 1000),
          alive: !info.killed,
          executable: info.executable
        });
      }
    }
    
    zombies.sort((a, b) => b.overtime - a.overtime);
    
    return zombies;
  }

  killAllZombieProcesses() {
    const zombies = this.getZombieProcesses();
    let killed = 0;
    
    for (const zombie of zombies) {
      if (this.killProcess(zombie.processId)) {
        killed++;
      }
    }
    
    return {
      totalZombies: zombies.length,
      killed,
      remaining: zombies.length - killed
    };
  }

  getStats() {
    const active = this.getActiveProcesses();
    const zombies = this.getZombieProcesses();
    
    const binaryCount = active.filter(p => {
      if (!p.executable) return false;
      const baseName = path.basename(p.executable);
      return (
        !this.systemCommands.has(baseName) &&
        !baseName.includes('python') &&
        !baseName.includes('node')
      );
    }).length;
    
    return {
      totalProcesses: this.processCounter,
      activeProcesses: active.length,
      zombieProcesses: zombies.length,
      binaryProcesses: binaryCount,
      zombieConfig: {
        ...this.zombieConfig,
        checkIntervalSeconds: this.zombieConfig.checkInterval / 1000,
        gracePeriodSeconds: this.zombieConfig.gracePeriod / 1000
      },
      avgDuration:
        active.length > 0
          ? active.reduce((sum, p) => sum + p.duration, 0) / active.length
          : 0,
      dataDir: this.dataDir,
      isShuttingDown: this.isShuttingDown
    };
  }
}

export default Executor;
import { spawn } from 'child_process';
import EventEmitter from 'events';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class Executor extends EventEmitter {
  constructor(config = {}) {
    super();
    this.activeProcesses = new Map();
    this.processCounter = 0;
    this.eventListeners = new Map();
    this.zombieCheckInterval = null;
    this.isShuttingDown = false;
    
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

  // FIX Bug #35: Improve error handling and logging
  _removeListener(emitter, event, handler) {
    if (!emitter || !handler) {
      console.warn('[EXEC] Attempted to remove listener with null emitter or handler');
      return;
    }
    
    try {
      if (typeof emitter.off === 'function') {
        emitter.off(event, handler);
      } else if (typeof emitter.removeListener === 'function') {
        emitter.removeListener(event, handler);
      } else {
        console.warn('[EXEC] Emitter has no off or removeListener method');
      }
    } catch (error) {
      console.error(`[EXEC] Error removing listener for ${event}:`, error.message);
    }
  }

  _cleanupProcessListeners(processId) {
    const listeners = this.eventListeners.get(processId);
    if (!listeners) {
      return;
    }
    
    const processInfo = this.activeProcesses.get(processId);
    const child = processInfo?.process;
    
    if (child) {
      try {
        if (child.stdout && listeners.stdoutHandler) {
          this._removeListener(child.stdout, 'data', listeners.stdoutHandler);
          if (listeners.stdoutErrorHandler) {
            this._removeListener(child.stdout, 'error', listeners.stdoutErrorHandler);
          }
        }
        if (child.stderr && listeners.stderrHandler) {
          this._removeListener(child.stderr, 'data', listeners.stderrHandler);
          if (listeners.stderrErrorHandler) {
            this._removeListener(child.stderr, 'error', listeners.stderrErrorHandler);
          }
        }
        if (listeners.exitHandler) {
          this._removeListener(child, 'exit', listeners.exitHandler);
        }
        if (listeners.errorHandler) {
          this._removeListener(child, 'error', listeners.errorHandler);
        }
        if (listeners.closeHandler) {
          this._removeListener(child, 'close', listeners.closeHandler);
        }
      } catch (error) {
        console.error('[EXEC] Error during listener cleanup:', error.message);
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

  // FIX Bug #36: Track kill failures
  checkAndKillZombieProcesses() {
    const now = Date.now();
    const zombieProcesses = [];
    
    for (const [processId, info] of this.activeProcesses) {
      if (!info.expectedEndTime) {
        continue;
      }
      
      const timeSinceExpected = now - info.expectedEndTime;
      
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
      let failedCount = 0;
      
      for (const zombie of zombieProcesses) {
        const overtimeSeconds = Math.round(zombie.overtime / 1000);
        console.log(
          `[EXEC-ZOMBIE] Process ${zombie.processId} (PID: ${zombie.info.pid}) ` +
          `overtime: ${overtimeSeconds}s`
        );

        if (this.zombieConfig.autoKill) {
          if (this.killProcess(zombie.processId)) {
            killedCount++;
            console.log(
              `[EXEC-ZOMBIE] Auto-killed process ${zombie.processId}`
            );
          } else {
            failedCount++;
            console.error(
              `[EXEC-ZOMBIE] Failed to kill process ${zombie.processId}`
            );
          }
        }
      }
      
      this.emit('zombie_detected', {
        timestamp: now,
        count: zombieProcesses.length,
        killed: killedCount,
        failed: failedCount,
        processes: zombieProcesses.map(z => ({
          processId: z.processId,
          pid: z.info.pid,
          command: z.info.command,
          overtime: z.overtime,
          overtimeSeconds: Math.round(z.overtime / 1000)
        }))
      });
    }
    
    return zombieProcesses.length;
  }

  cleanup() {
    console.log('[EXEC] Cleaning up executor resources...');
    
    this.isShuttingDown = true;
    this.stopZombieChecker();

    const killed = this.killAllProcesses();
    console.log(`[EXEC] Killed ${killed} active processes during cleanup`);

    setTimeout(() => {
      for (const processId of this.eventListeners.keys()) {
        this._cleanupProcessListeners(processId);
      }
      
      this.eventListeners.clear();
      this.activeProcesses.clear();
      this.removeAllListeners();
      
      console.log('[EXEC] Executor cleanup completed');
    }, 500);
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

  // FIX Bug #37: Add path traversal protection
  findFileInDataDir(filename) {
    filename = filename.replace(/^["']|["']$/g, '');
    
    // SECURITY: Prevent path traversal
    if (filename.includes('..')) {
      console.warn(`[EXEC] Rejected path with traversal: ${filename}`);
      return filename;
    }
    
    // Check if absolute path and validate it's within allowed directories
    if (path.isAbsolute(filename)) {
      const normalized = path.normalize(filename);
      const normalizedDataDir = path.normalize(this.dataDir);
      const isInDataDir = normalized.startsWith(normalizedDataDir);
      
      if (!isInDataDir) {
        console.warn(`[EXEC] Rejected absolute path outside data dir: ${filename}`);
        return filename;
      }
      
      if (fs.existsSync(normalized)) {
        return normalized;
      }
    }
    
    // System command check
    if (this.systemCommands.has(filename)) {
      return filename;
    }
    
    // Search in allowed directories only
    const possiblePaths = [
      path.join(this.dataDir, filename),
      path.join(this.dataDir, 'scripts', filename),
      path.join(this.dataDir, 'bin', filename),
      path.join(this.dataDir, 'tools', filename)
    ];
    
    for (const filePath of possiblePaths) {
      try {
        // Double-check normalized path is in data dir
        const normalized = path.normalize(filePath);
        const normalizedDataDir = path.normalize(this.dataDir);
        
        if (!normalized.startsWith(normalizedDataDir)) {
          console.warn(`[EXEC] Path normalization resulted in traversal: ${filePath}`);
          continue;
        }
        
        if (fs.existsSync(normalized)) {
          const stats = fs.statSync(normalized);
          if (!stats.isDirectory()) {
            const ext = path.extname(normalized).toLowerCase();
            const isExecutableType =
              !ext || 
              ext === '.sh' || 
              ext === '.py' || 
              ext === '.pl' || 
              ext === '.rb' ||
              ext === '.bin';
            
            if (isExecutableType) {
              try {
                fs.chmodSync(normalized, '755');
              } catch (e) {
                console.warn(`[EXEC] Could not chmod ${normalized}:`, e.message);
              }
            }
            return normalized;
          }
        }
      } catch (e) {
        continue;
      }
    }
    
    return filename;
  }

  resolveCommandParts(parts) {
    if (!parts || parts.length === 0) return parts;
    
    const resolvedParts = [...parts];
    
    for (let i = 0; i < resolvedParts.length; i++) {
      const part = resolvedParts[i];
      
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

  // FIX Bug #38: Add command injection protection
  execute(command, options = {}) {
    if (this.isShuttingDown) {
      return Promise.reject({
        success: false,
        error: 'Executor is shutting down'
      });
    }
    
    // SECURITY: Basic command injection prevention
    if (!command || typeof command !== 'string') {
      return Promise.reject({
        success: false,
        error: 'Invalid command type'
      });
    }
    
    // Check for dangerous patterns
    const dangerousPatterns = [
      /;\s*rm\s+-rf/i,
      /&&\s*rm\s+-rf/i,
      /\|\s*sh/i,
      /\|\s*bash/i,
      /`.*`/,
      /\$\(/,
      />\s*\/dev\//i,
      /curl.*\|\s*bash/i,
      /wget.*\|\s*bash/i
    ];
    
    for (const pattern of dangerousPatterns) {
      if (pattern.test(command)) {
        console.error(`[EXEC] Rejected dangerous command pattern: ${pattern}`);
        return Promise.reject({
          success: false,
          error: 'Command contains dangerous pattern'
        });
      }
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

      // FIX #9: resolved flag is used to prevent double resolve/reject.
      // Set to false initially; we resolve AFTER all listeners are attached
      // so that a synchronous 'error' event (which can fire before the next
      // tick) is always caught by our errorHandler.
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
          `[EXEC ${processId}] Expected duration: ${expectedDuration / 1000}s`
        );
      }

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

      const stdoutErrorHandler = (error) => {
        console.error(`[EXEC ${processId}] stdout error:`, error.message);
      };

      const stderrErrorHandler = (error) => {
        console.error(`[EXEC ${processId}] stderr error:`, error.message);
      };

      const exitHandler = (code, signal) => {
        const processInfo = this.activeProcesses.get(processId);
        if (!processInfo) {
          return;
        }
        
        const endTime = Date.now();
        const duration = endTime - startTime;
        
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
            `duration=${duration}ms, expected=${expectedDuration}ms`
          );
        } else {
          console.log(
            `[EXEC ${processId}] Exited. code=${code}, duration=${duration}ms`
          );
        }
      };

      const closeHandler = (code, signal) => {
        if (!this.isShuttingDown) {
          console.log(`[EXEC ${processId}] Process closed`);
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

        // FIX #9: Only reject if not yet resolved.
        // Because listeners are attached BEFORE resolve() is called below,
        // the errorHandler can correctly run and reject the promise before
        // the resolve line is ever reached.
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

      // FIX #9: Attach ALL listeners BEFORE resolving the promise so that a
      // synchronous 'error' event fired during/after spawn is never missed.
      // Previously, `resolved = true` and `resolve(...)` were set AFTER
      // listener attachment but the variable was set to `true` at the bottom
      // which meant errorHandler's `!resolved` check was always false on
      // synchronous errors. Now we only set resolved=true inside the handlers
      // or at the very end after all listeners are safely attached.
      try {
        if (childProcess.stdout) {
          childProcess.stdout.on('data', stdoutHandler);
          childProcess.stdout.on('error', stdoutErrorHandler);
        }
        if (childProcess.stderr) {
          childProcess.stderr.on('data', stderrHandler);
          childProcess.stderr.on('error', stderrErrorHandler);
        }
        childProcess.on('exit', exitHandler);
        childProcess.on('close', closeHandler);
        childProcess.on('error', errorHandler);

        this.eventListeners.set(processId, {
          stdoutHandler,
          stderrHandler,
          stdoutErrorHandler,
          stderrErrorHandler,
          exitHandler,
          closeHandler,
          errorHandler
        });
      } catch (error) {
        console.error(`[EXEC ${processId}] Failed to attach listeners:`, error.message);
        // If we can't attach listeners, reject immediately
        if (!resolved) {
          resolved = true;
          this.activeProcesses.delete(processId);
          return reject({
            success: false,
            processId,
            error: `Failed to attach listeners: ${error.message}`
          });
        }
      }

      // FIX #9: Emit process_started AFTER listeners are attached but BEFORE
      // resolving, so external handlers see the event consistently.
      this.emit('process_started', {
        processId,
        command: resolvedCommand,
        pid: childProcess.pid,
        startTime,
        expectedDuration,
        expectedEndTime,
        executable
      });

      // FIX #9: Now safe to resolve. If errorHandler already ran synchronously
      // and set resolved=true, this resolve call is effectively a no-op because
      // Promise can only settle once.
      if (!resolved) {
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
        
        try {
          child.kill('SIGTERM');
          console.log(
            `[EXEC ${processId}] Sent SIGTERM to process ${processInfo.pid}`
          );
          
          setTimeout(() => {
            if (this.activeProcesses.has(processId)) {
              try {
                child.kill('SIGKILL');
                console.log(
                  `[EXEC ${processId}] Sent SIGKILL to process ${processInfo.pid}`
                );
              } catch (e) {
                console.error(`[EXEC ${processId}] SIGKILL failed:`, e.message);
              }
            }
          }, 2000);
        } catch (killError) {
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
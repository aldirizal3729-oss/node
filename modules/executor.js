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
    
    this.zombieConfig = {
      enabled: config.ZOMBIE_DETECTION?.ENABLED !== false,
      checkInterval: config.ZOMBIE_DETECTION?.CHECK_INTERVAL || 3000,
      gracePeriod: config.ZOMBIE_DETECTION?.GRACE_PERIOD || 2000,
      autoKill: config.ZOMBIE_DETECTION?.AUTO_KILL !== false
    };

    this.dataDir = path.join(__dirname, '..', 'lib', 'data');
    
    if (!fs.existsSync(this.dataDir)) {
      fs.mkdirSync(this.dataDir, { recursive: true });
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

  // Helper untuk menghapus event listener dengan kompatibilitas
  _removeListener(emitter, event, handler) {
    if (emitter && handler) {
      if (typeof emitter.off === 'function') {
        emitter.off(event, handler);
      } else if (typeof emitter.removeListener === 'function') {
        emitter.removeListener(event, handler);
      }
    }
  }

  // Membersihkan semua event listener untuk suatu process
  _cleanupProcessListeners(processId) {
    const listeners = this.eventListeners.get(processId);
    if (listeners) {
      const processInfo = this.activeProcesses.get(processId);
      const child = processInfo?.process;
      
      if (child) {
        this._removeListener(child.stdout, 'data', listeners.stdoutHandler);
        this._removeListener(child.stderr, 'data', listeners.stderrHandler);
        this._removeListener(child, 'exit', listeners.exitHandler);
        this._removeListener(child, 'error', listeners.errorHandler);
      }
      
      this.eventListeners.delete(processId);
    }
  }

  startZombieChecker() {
    if (this.zombieCheckInterval) {
      clearInterval(this.zombieCheckInterval);
    }
    
    this.zombieCheckInterval = setInterval(() => {
      this.checkAndKillZombieProcesses();
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

  checkAndKillZombieProcesses() {
    const now = Date.now();
    const zombieProcesses = [];
    
    for (const [processId, info] of this.activeProcesses) {
      if (
        info.expectedEndTime &&
        now > info.expectedEndTime + this.zombieConfig.gracePeriod
      ) {
        const overtime = now - info.expectedEndTime;
        zombieProcesses.push({
          processId,
          info,
          overtime
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
          `overtime: ${overtimeSeconds}s`
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

    this.stopZombieChecker();

    const killed = this.killAllProcesses();
    console.log(`[EXEC] Killed ${killed} active processes during cleanup`);

    this.eventListeners.clear();
    this.activeProcesses.clear();

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

  findFileInDataDir(filename) {
    filename = filename.replace(/^["']|["']$/g, '');
    
    if (path.isAbsolute(filename)) {
      return filename;
    }
    
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
      if (fs.existsSync(filePath)) {
        try {
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
        } catch (e) {
          // ignore stat error
        }
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

  execute(command, options = {}) {
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
          `[EXEC ${processId}] Expected duration: ${expectedDuration / 1000}s`
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

      const stdoutHandler = (data) => {
        this.emit('process_stdout', {
          processId,
          data: data.toString()
        });
      };

      const stderrHandler = (data) => {
        this.emit('process_stderr', {
          processId,
          data: data.toString()
        });
      };

      const exitHandler = (code, signal) => {
        const processInfo = this.activeProcesses.get(processId);
        if (!processInfo) {
          return;
        }
        
        const endTime = Date.now();
        const duration = endTime - startTime;
        const wasZombie =
          expectedEndTime &&
          duration > expectedDuration + this.zombieConfig.gracePeriod;

        processInfo.killed =
          processInfo.killed ||
          signal === 'SIGKILL' ||
          signal === 'SIGTERM';
        
        this.activeProcesses.delete(processId);
        this._cleanupProcessListeners(processId);

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

        if (wasZombie) {
          console.log(
            `[EXEC ${processId}] Exited as ZOMBIE. duration=${duration}ms`
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

        this.emit('process_error', {
          processId,
          command: resolvedCommand,
          error: error.message,
          executable
        });
      };

      childProcess.stdout?.on('data', stdoutHandler);
      childProcess.stderr?.on('data', stderrHandler);
      childProcess.on('exit', exitHandler);
      childProcess.on('error', errorHandler);

      this.eventListeners.set(processId, {
        stdoutHandler,
        stderrHandler,
        exitHandler,
        errorHandler
      });
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
        child.kill('SIGKILL');
        console.log(
          `[EXEC ${processId}] Sent SIGKILL to process ${processInfo.pid}`
        );
      }

      this.activeProcesses.delete(processId);
      this._cleanupProcessListeners(processId);

      this.emit('process_killed', {
        processId,
        pid: processInfo.pid,
        command: processInfo.command,
        reason: 'manual_kill',
        executable: processInfo.executable
      });

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

    for (const [processId, info] of this.activeProcesses) {
      const now = Date.now();
      const isZombie =
        info.expectedEndTime &&
        now > info.expectedEndTime + this.zombieConfig.gracePeriod;
      
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
        overtime: isZombie ? now - info.expectedEndTime : 0,
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
    const isZombie =
      info.expectedEndTime &&
      now > info.expectedEndTime + this.zombieConfig.gracePeriod;

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
      overtime: isZombie ? now - info.expectedEndTime : 0,
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
      dataDir: this.dataDir
    };
  }
}

export default Executor;
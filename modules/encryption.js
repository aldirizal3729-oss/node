import crypto from 'crypto';

class EncryptionManager {
  constructor(config) {
    // Initialize all properties first before validation
    this.config = {
      ENABLED: true,
      VERSION: '1.0',
      ALGORITHM: 'aes-256-gcm',
      SALT_LENGTH: 16,
      IV_LENGTH: 12,
      AUTH_TAG_LENGTH: 16,
      KEY_DERIVATION: {
        ITERATIONS: 100000,
        KEYLENGTH: 32,
        DIGEST: 'sha256'
      },
      SHARED_SECRET: null,
      NODE_ID: null
    };
    
    // Apply configuration
    if (config && config.ENCRYPTION) {
      Object.assign(this.config, config.ENCRYPTION);
    }
    
    if (config && config.NODE && config.NODE.ID) {
      this.config.NODE_ID = config.NODE.ID;
    }
    
    // Set shared secret
    this.sharedSecret = null;
    if (this.config.SHARED_SECRET) {
      this.sharedSecret = this.config.SHARED_SECRET;
    } else if (config && config.SHARED_SECRET) {
      this.sharedSecret = config.SHARED_SECRET;
    }
    
    // FIX Bug #9: Validate AFTER all initialization, disable before throwing
    if (this.config.ENABLED && !this.sharedSecret) {
      this.config.ENABLED = false; // Disable to prevent partial initialization
      throw new Error('SHARED_SECRET is required when encryption is enabled');
    }
    
    console.log('[ENCRYPTION] Initialized', {
      enabled: this.config.ENABLED,
      version: this.config.VERSION,
      hasSecret: !!this.sharedSecret,
      nodeId: this.config.NODE_ID
    });
  }

  generateKey(salt) {
    try {
      if (!salt || !Buffer.isBuffer(salt)) {
        throw new Error('Invalid salt: must be a Buffer');
      }
      
      if (salt.length !== this.config.SALT_LENGTH) {
        console.warn(
          `[ENCRYPTION] Warning: Salt length mismatch. Expected ${this.config.SALT_LENGTH}, got ${salt.length}`
        );
      }
      
      if (!this.sharedSecret) {
        throw new Error('Shared secret not configured');
      }
      
      const key = crypto.pbkdf2Sync(
        this.sharedSecret,
        salt,
        this.config.KEY_DERIVATION.ITERATIONS,
        this.config.KEY_DERIVATION.KEYLENGTH,
        this.config.KEY_DERIVATION.DIGEST
      );
      
      return key;
    } catch (error) {
      console.error('[ENCRYPTION] Key generation error:', error.message);
      throw error;
    }
  }

  encrypt(data) {
    try {
      if (!data) {
        throw new Error('No data to encrypt');
      }
      
      if (!this.config.ENABLED || !this.sharedSecret) {
        throw new Error('Encryption is not enabled or configured');
      }
      
      const salt = crypto.randomBytes(this.config.SALT_LENGTH);
      const iv = crypto.randomBytes(this.config.IV_LENGTH);
      const key = this.generateKey(salt);
      
      const dataToEncrypt = typeof data === 'string' ? data : JSON.stringify(data);
      
      const cipher = crypto.createCipheriv(
        this.config.ALGORITHM,
        key,
        iv,
        { authTagLength: this.config.AUTH_TAG_LENGTH }
      );
      
      let encrypted = cipher.update(dataToEncrypt, 'utf8', 'hex');
      encrypted += cipher.final('hex');
      const authTag = cipher.getAuthTag();
      
      const result = {
        encrypted,
        iv: iv.toString('hex'),
        salt: salt.toString('hex'),
        authTag: authTag.toString('hex'),
        timestamp: Date.now(),
        version: this.config.VERSION,
        nodeId: this.config.NODE_ID
      };
      
      return result;
    } catch (error) {
      console.error('[ENCRYPTION] Encryption error:', error.message);
      throw error;
    }
  }

  decrypt(encryptedPackage) {
    try {
      if (!encryptedPackage || typeof encryptedPackage !== 'object') {
        throw new Error('Invalid encrypted package: not an object');
      }
      
      if (!this.config.ENABLED || !this.sharedSecret) {
        throw new Error('Encryption is not enabled or configured');
      }
      
      const { encrypted, iv, salt, authTag } = encryptedPackage;
      
      if (!encrypted || typeof encrypted !== 'string' || encrypted.length === 0) {
        throw new Error('Missing or invalid encrypted data');
      }
      
      if (!iv || typeof iv !== 'string' || iv.length === 0) {
        throw new Error('Missing or invalid IV');
      }
      
      if (!salt || typeof salt !== 'string' || salt.length === 0) {
        throw new Error('Missing or invalid salt');
      }
      
      if (!authTag || typeof authTag !== 'string' || authTag.length === 0) {
        throw new Error('Missing or invalid authTag');
      }
      
      let ivBuffer, saltBuffer, authTagBuffer;
      try {
        ivBuffer = Buffer.from(iv, 'hex');
        saltBuffer = Buffer.from(salt, 'hex');
        authTagBuffer = Buffer.from(authTag, 'hex');
      } catch (bufferError) {
        throw new Error(`Invalid hex encoding: ${bufferError.message}`);
      }
      
      if (ivBuffer.length !== this.config.IV_LENGTH) {
        console.warn(
          `[ENCRYPTION] Warning: IV length mismatch. Expected ${this.config.IV_LENGTH}, got ${ivBuffer.length}`
        );
      }
      
      if (saltBuffer.length !== this.config.SALT_LENGTH) {
        console.warn(
          `[ENCRYPTION] Warning: Salt length mismatch. Expected ${this.config.SALT_LENGTH}, got ${saltBuffer.length}`
        );
      }
      
      if (authTagBuffer.length !== this.config.AUTH_TAG_LENGTH) {
        console.warn(
          `[ENCRYPTION] Warning: AuthTag length mismatch. Expected ${this.config.AUTH_TAG_LENGTH}, got ${authTagBuffer.length}`
        );
      }
      
      const key = this.generateKey(saltBuffer);
      
      const decipher = crypto.createDecipheriv(
        this.config.ALGORITHM,
        key,
        ivBuffer,
        { authTagLength: this.config.AUTH_TAG_LENGTH }
      );
      
      decipher.setAuthTag(authTagBuffer);
      
      let decrypted = decipher.update(encrypted, 'hex', 'utf8');
      decrypted += decipher.final('utf8');
      
      try {
        return JSON.parse(decrypted);
      } catch {
        return decrypted;
      }
    } catch (error) {
      console.error('[ENCRYPTION] Decryption error:', error.message);
      console.error('[ENCRYPTION] Error details:', {
        hasPackage: !!encryptedPackage,
        packageKeys: encryptedPackage ? Object.keys(encryptedPackage) : 'no package',
        hasEncrypted: !!(encryptedPackage && encryptedPackage.encrypted),
        hasIv: !!(encryptedPackage && encryptedPackage.iv),
        hasSalt: !!(encryptedPackage && encryptedPackage.salt),
        hasAuthTag: !!(encryptedPackage && encryptedPackage.authTag)
      });
      throw error;
    }
  }

  createSecureMessage(data, messageType) {
    if (!this.config.ENABLED || !this.sharedSecret) {
      return {
        envelope: 'plain',
        version: this.config.VERSION,
        payload: data,
        metadata: {
          messageType,
          timestamp: Date.now(),
          algorithm: 'none',
          nodeId: this.config.NODE_ID || 'unknown',
          encrypted: false
        }
      };
    }

    const message = {
      nodeId: this.config.NODE_ID || 'unknown',
      type: messageType,
      data,
      timestamp: Date.now(),
      nonce: crypto.randomBytes(16).toString('hex')
    };

    const signature = this.createSignature(message);
    message.signature = signature;

    const encrypted = this.encrypt(message);
    
    const envelope = {
      envelope: 'secure',
      version: this.config.VERSION,
      payload: encrypted,
      metadata: {
        messageType,
        timestamp: Date.now(),
        algorithm: this.config.ALGORITHM,
        nodeId: this.config.NODE_ID || 'unknown',
        encrypted: true
      }
    };
    
    return envelope;
  }

  processSecureMessage(envelope) {
    try {
      if (!envelope || typeof envelope !== 'object') {
        return {
          success: false,
          error: 'Invalid message envelope',
          shouldFallback: true
        };
      }
      
      if (envelope.envelope === 'plain') {
        // FIX Bug #11: Validate payload exists
        if (!envelope.payload) {
          return {
            success: false,
            error: 'Plain envelope missing payload',
            shouldFallback: true
          };
        }
        
        return {
          success: true,
          data: envelope.payload,
          type: envelope.metadata?.messageType || 'unknown',
          encrypted: false,
          metadata: envelope.metadata || {}
        };
      }
      
      if (envelope.envelope !== 'secure') {
        return {
          success: false,
          error: 'Invalid message envelope type',
          shouldFallback: true
        };
      }

      // FIX Bug #11: Validate secure envelope has payload
      if (!envelope.payload) {
        return {
          success: false,
          error: 'Secure envelope missing payload',
          shouldFallback: false
        };
      }

      const decrypted = this.decrypt(envelope.payload);
      
      if (decrypted.signature) {
        if (!this.verifySignature(decrypted, decrypted.signature)) {
          throw new Error('Signature verification failed');
        }
      }

      const now = Date.now();
      if (decrypted.timestamp && Math.abs(now - decrypted.timestamp) > 300000) {
        console.warn(
          `[ENCRYPTION] Message timestamp outside acceptable window: ${Math.abs(
            now - decrypted.timestamp
          )}ms diff`
        );
      }

      return {
        success: true,
        data: decrypted.data || decrypted,
        type: decrypted.type || 'unknown',
        encrypted: true,
        metadata: {
          timestamp: decrypted.timestamp,
          nonce: decrypted.nonce,
          nodeId: decrypted.nodeId
        }
      };
    } catch (error) {
      console.error('[ENCRYPTION] Message processing error:', error.message);
      
      if (
        error.message.includes('not enabled') ||
        error.message.includes('not configured')
      ) {
        return {
          success: false,
          error: error.message,
          shouldFallback: true,
          rawError: error
        };
      }
      
      return {
        success: false,
        error: error.message,
        shouldFallback: false,
        rawError: error
      };
    }
  }

  createSignature(data) {
    try {
      if (!this.sharedSecret) {
        return '';
      }
      
      const dataToSign = { ...data };
      delete dataToSign.signature;
      
      const hmac = crypto.createHmac('sha256', this.sharedSecret);
      hmac.update(JSON.stringify(dataToSign));
      return hmac.digest('hex');
    } catch (error) {
      console.error('[ENCRYPTION] Signature creation error:', error.message);
      return '';
    }
  }

  verifySignature(data, signature) {
    try {
      if (!this.sharedSecret || !signature) {
        return false;
      }
      
      const dataToVerify = { ...data };
      delete dataToVerify.signature;
      
      const expected = this.createSignature(dataToVerify);
      
      if (!expected) {
        return false;
      }
      
      const expectedBuffer = Buffer.from(expected, 'hex');
      const signatureBuffer = Buffer.from(signature, 'hex');
      
      // FIX Bug #10: Check length BEFORE timingSafeEqual to prevent timing attacks
      if (expectedBuffer.length !== signatureBuffer.length) {
        return false;
      }
      
      return crypto.timingSafeEqual(expectedBuffer, signatureBuffer);
    } catch (error) {
      console.error('[ENCRYPTION] Signature verification error:', error.message);
      return false;
    }
  }

  quickEncrypt(data) {
    return this.createSecureMessage(data, 'data');
  }

  quickDecrypt(envelope) {
    try {
      const result = this.processSecureMessage(envelope);
      return result.success ? result.data : null;
    } catch (error) {
      console.error('[ENCRYPTION] Quick decrypt error:', error.message);
      return null;
    }
  }

  testEncryption() {
    try {
      if (!this.config.ENABLED || !this.sharedSecret) {
        return {
          success: true,
          enabled: false,
          message: 'Encryption is disabled'
        };
      }
      
      const testData = {
        message: 'Encryption test',
        timestamp: Date.now(),
        nodeId: this.config.NODE_ID || 'test',
        test: true
      };
      
      const encrypted = this.encrypt(testData);
      const decrypted = this.decrypt(encrypted);
      
      const testDataStr = JSON.stringify(testData);
      const decryptedStr =
        typeof decrypted === 'string' ? decrypted : JSON.stringify(decrypted);
      
      const success = testDataStr === decryptedStr;
      
      if (!success) {
        console.warn('[ENCRYPTION] Self-test failed:');
        console.warn('Original:', testData);
        console.warn('Decrypted:', decrypted);
      }
      
      return {
        success,
        enabled: true,
        testData,
        encrypted,
        decrypted
      };
    } catch (error) {
      console.error('[ENCRYPTION] Self-test error:', error.message);
      return {
        success: false,
        enabled: this.config.ENABLED,
        error: error.message
      };
    }
  }
}

export default EncryptionManager;

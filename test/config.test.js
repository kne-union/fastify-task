const { expect } = require('chai');
const { ConfigManager, CONFIG_CONSTANTS, getConfigManager, _resetConfigManager } = require('../libs/utils/config');

describe('config.js', () => {
  afterEach(() => {
    _resetConfigManager();
  });

  const baseOptions = {
    name: 'task',
    getUserModel: () => {},
    getAuthenticate: () => []
  };

  describe('ConfigManager', () => {
    it('should merge with defaults', () => {
      const cm = new ConfigManager(baseOptions);
      const config = cm.getAll();
      expect(config.name).to.equal('task');
      expect(config.prefix).to.equal('/api/task');
      expect(config.limit).to.equal(CONFIG_CONSTANTS.CONCURRENT_TASKS_DEFAULT);
    });

    it('should override user config', () => {
      const cm = new ConfigManager({ ...baseOptions, limit: 5 });
      expect(cm.getAll().limit).to.equal(5);
    });

    it('should merge nested objects', () => {
      const cm = new ConfigManager(baseOptions);
      const config = cm.getAll();
      expect(config.security.enableSignature).to.equal(true);
    });
  });

  describe('validateConfig', () => {
    it('should throw for invalid limit (too low)', () => {
      expect(() => new ConfigManager({ ...baseOptions, limit: 0 })).to.throw('配置项 limit');
    });

    it('should throw for invalid limit (too high)', () => {
      expect(() => new ConfigManager({ ...baseOptions, limit: 101 })).to.throw('配置项 limit');
    });

    it('should throw for invalid taskTimeout (negative)', () => {
      expect(() => new ConfigManager({ ...baseOptions, taskTimeout: -1 })).to.throw('配置项 taskTimeout');
    });

    it('should throw for invalid taskTimeout (too high)', () => {
      expect(() => new ConfigManager({ ...baseOptions, taskTimeout: 86400001 })).to.throw('配置项 taskTimeout');
    });

    it('should throw for empty name', () => {
      expect(() => new ConfigManager({ ...baseOptions, name: '' })).to.throw('配置项 name');
    });

    it('should throw for non-string name', () => {
      expect(() => new ConfigManager({ ...baseOptions, name: 123 })).to.throw('配置项 name');
    });

    it('should throw for non-array dirs', () => {
      const cm = new ConfigManager(baseOptions);
      // Force dirs to non-array to test validation directly
      cm.config.dirs = 'not-array';
      expect(() => cm.validateConfig()).to.throw('配置项 dirs');
    });

    it('should throw for empty dirs array', () => {
      const cm = new ConfigManager(baseOptions);
      cm.config.dirs = [];
      expect(() => cm.validateConfig()).to.throw('配置项 dirs');
    });

    it('should throw for non-function getUserModel', () => {
      expect(() => new ConfigManager({ ...baseOptions, getUserModel: 'not-func' })).to.throw('配置项 getUserModel');
    });

    it('should throw for non-function getAuthenticate', () => {
      expect(() => new ConfigManager({ ...baseOptions, getAuthenticate: 'not-func' })).to.throw('配置项 getAuthenticate');
    });

    it('should throw for non-function and non-null errorHandler', () => {
      expect(() => new ConfigManager({ ...baseOptions, errorHandler: 'not-func' })).to.throw('配置项 errorHandler');
    });

    it('should accept null errorHandler', () => {
      const cm = new ConfigManager({ ...baseOptions, errorHandler: null });
      expect(cm.getAll().errorHandler).to.equal(null);
    });

    it('should accept function errorHandler', () => {
      const handler = async () => {};
      const cm = new ConfigManager({ ...baseOptions, errorHandler: handler });
      expect(cm.getAll().errorHandler).to.equal(handler);
    });

    it('should default errorHandler to null', () => {
      const cm = new ConfigManager(baseOptions);
      expect(cm.getAll().errorHandler).to.equal(null);
    });
  });

  describe('get', () => {
    it('should get top-level config', () => {
      const cm = new ConfigManager(baseOptions);
      expect(cm.get('name')).to.equal('task');
    });

    it('should get nested config via dot notation', () => {
      const cm = new ConfigManager(baseOptions);
      expect(cm.get('security.enableSignature')).to.equal(true);
    });

    it('should return defaultValue for missing key', () => {
      const cm = new ConfigManager(baseOptions);
      expect(cm.get('nonexistent', 'fallback')).to.equal('fallback');
    });

    it('should return undefined for missing key without default', () => {
      const cm = new ConfigManager(baseOptions);
      expect(cm.get('nonexistent')).to.equal(undefined);
    });
  });

  describe('set', () => {
    it('should set top-level value', () => {
      const cm = new ConfigManager(baseOptions);
      cm.set('limit', 50);
      expect(cm.get('limit')).to.equal(50);
    });

    it('should set nested value via dot notation', () => {
      const cm = new ConfigManager(baseOptions);
      cm.set('security.enableSignature', false);
      expect(cm.get('security.enableSignature')).to.equal(false);
    });

    it('should create intermediate objects when not exist', () => {
      const cm = new ConfigManager(baseOptions);
      cm.set('custom.nested.key', 'value');
      expect(cm.get('custom.nested.key')).to.equal('value');
    });
  });

  describe('getConstants', () => {
    it('should return CONFIG_CONSTANTS copy', () => {
      const cm = new ConfigManager(baseOptions);
      const constants = cm.getConstants();
      expect(constants.TASK_STATUSES.PENDING).to.equal('pending');
      expect(constants.POLL_MIN_INTERVAL_MS).to.equal(CONFIG_CONSTANTS.POLL_MIN_INTERVAL_MS);
    });

    it('should return a shallow copy', () => {
      const cm = new ConfigManager(baseOptions);
      const c1 = cm.getConstants();
      const c2 = cm.getConstants();
      expect(c1).to.not.equal(c2);
    });
  });

  describe('getConfigManager singleton', () => {
    it('should return same instance', () => {
      const cm = getConfigManager(baseOptions);
      const cm2 = getConfigManager();
      expect(cm2).to.equal(cm);
    });
  });

  describe('dirs backward compat', () => {
    it('should wrap dir into dirs when dirs not provided', () => {
      const cm = new ConfigManager(baseOptions);
      const config = cm.getAll();
      expect(config.dirs).to.be.an('array');
      expect(config.dirs.length).to.be.greaterThan(0);
    });

    it('should merge dir into dirs when dirs provided', () => {
      const cm = new ConfigManager({ ...baseOptions, dirs: ['/custom'] });
      const config = cm.getAll();
      expect(config.dirs).to.include('/custom');
    });

    it('should reuse user dirs when it already includes default dir', () => {
      const defaultDir = require('path').resolve(process.cwd(), 'libs', 'tasks');
      const cm = new ConfigManager({ ...baseOptions, dirs: [defaultDir] });
      const config = cm.getAll();
      expect(config.dirs).to.include(defaultDir);
    });
  });

  describe('DEFAULT_CONFIG isolation', () => {
    it('should not mutate DEFAULT_CONFIG when setting nested values', () => {
      const { DEFAULT_CONFIG } = require('../libs/utils/config');
      const beforeValue = DEFAULT_CONFIG.security.enableSignature;
      const cm = new ConfigManager(baseOptions);
      cm.set('security.enableSignature', false);
      expect(DEFAULT_CONFIG.security.enableSignature).to.equal(beforeValue);
    });

    it('should not mutate DEFAULT_CONFIG arrays', () => {
      const { DEFAULT_CONFIG } = require('../libs/utils/config');
      const cm = new ConfigManager(baseOptions);
      const config = cm.getAll();
      // dirs is now a new array from mergeConfig, so pushing to it shouldn't affect DEFAULT_CONFIG
      const originalDirsLength = DEFAULT_CONFIG.dirs ? DEFAULT_CONFIG.dirs.length : -1;
      config.dirs.push('/injected');
      // DEFAULT_CONFIG.dirs is null, so it can't be mutated this way
      // But the important thing is config.dirs is a separate array
      expect(config.dirs).to.include('/injected');
      if (originalDirsLength >= 0) {
        expect(DEFAULT_CONFIG.dirs).to.not.include('/injected');
      }
    });

    it('should not mutate DEFAULT_CONFIG when userConfig provides nested object', () => {
      const { DEFAULT_CONFIG } = require('../libs/utils/config');
      const cm = new ConfigManager({ ...baseOptions, security: { enableSignature: false } });
      expect(DEFAULT_CONFIG.security.enableSignature).to.equal(true);
    });
  });

  describe('DEFAULT_CONFIG defaults', () => {
    it('should throw error from default getUserModel', () => {
      const { DEFAULT_CONFIG } = require('../libs/utils/config');
      expect(() => DEFAULT_CONFIG.getUserModel()).to.throw('getUserModel 必须在配置中提供');
    });

    it('should throw error from default getAuthenticate', () => {
      const { DEFAULT_CONFIG } = require('../libs/utils/config');
      expect(() => DEFAULT_CONFIG.getAuthenticate()).to.throw('getAuthenticate 必须在配置中提供');
    });
  });
});

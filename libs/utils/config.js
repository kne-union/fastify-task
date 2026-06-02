const path = require('node:path');

/**
 * 配置常量定义
 */
const CONFIG_CONSTANTS = {
  // 日志相关
  LOGS_MAX_SIZE: 100,
  LOGS_RETENTION_DAYS: 30,

  // 轮询相关
  POLL_MAX_TIMES: 20,
  POLL_INTERVAL_MS: 10000,
  POLL_MIN_INTERVAL_MS: 10,
  POLL_MAX_INTERVAL_MS: 60000,

  // 任务超时相关
  TASK_TIMEOUT_DEFAULT: 30 * 60 * 1000, // 30分钟
  TASK_TIMEOUT_MIN: 0, // 0表示不超时
  TASK_TIMEOUT_MAX: 24 * 60 * 60 * 1000, // 24小时

  // 重试相关
  RETRY_BASE_DELAY: 5000, // 5秒
  RETRY_MAX_DELAY: 300000, // 5分钟
  RETRY_MAX_COUNT: 10,

  // 并发相关
  CONCURRENT_TASKS_DEFAULT: 10,
  CONCURRENT_TASKS_MIN: 1,
  CONCURRENT_TASKS_MAX: 100,

  // 优先级相关
  PRIORITY_MIN: 0,
  PRIORITY_MAX: 100,
  PRIORITY_DEFAULT: 0,

  // 延迟相关
  DELAY_MIN: 0,
  DELAY_MAX: 86400, // 24小时

  // 文件相关
  SCRIPT_EXTENSION: '.js',
  SCRIPT_NAME_DEFAULT: 'index',

  // 签名相关
  SIGNATURE_ALGORITHM: 'sha256',
  SIGNATURE_MIN_LENGTH: 32,

  // 状态相关
  TASK_STATUSES: {
    PENDING: 'pending',
    RUNNING: 'running',
    WAITING: 'waiting',
    SUCCESS: 'success',
    FAILED: 'failed',
    CANCELED: 'canceled'
  },

  // 执行者类型
  RUNNER_TYPES: {
    SYSTEM: 'system',
    MANUAL: 'manual'
  },

  // 时间范围
  TIME_RANGES: {
    '7d': { value: 7, unit: 'day', label: '近7天' },
    '1m': { value: 1, unit: 'month', label: '近1个月' },
    '3m': { value: 3, unit: 'month', label: '近3个月' },
    '1y': { value: 1, unit: 'year', label: '近1年' }
  },

  // 统计相关
  STATISTICS_ATTRIBUTES: ['total', 'success', 'failed', 'canceled', 'waitingTime', 'executionTime', 'totalTime'],
  STATISTICS_PERIODS: ['h', 'd'],

  // 数据库相关
  DB_TABLE_PREFIX_DEFAULT: 't_',
  DB_INDEXES: ['created_at', 'status', 'type', 'runner_type', ['type', 'status'], ['runner_type', 'status'], ['target_id', 'target_type', 'type'], ['parent_task_id'], ['priority']]
};

/**
 * 默认配置
 */
const DEFAULT_CONFIG = {
  // 数据库配置
  dbTableNamePrefix: CONFIG_CONSTANTS.DB_TABLE_PREFIX_DEFAULT,

  // API配置
  prefix: '/api/task',
  name: 'task',

  // 并发配置
  limit: CONFIG_CONSTANTS.CONCURRENT_TASKS_DEFAULT,

  // 目录配置
  dir: path.resolve(process.cwd(), 'libs', 'tasks'),
  dirs: null,

  // 定时任务配置
  cronTime: '*/10 * * * *',

  // 脚本配置
  scriptName: CONFIG_CONSTANTS.SCRIPT_NAME_DEFAULT,

  // 轮询配置
  maxPollTimes: CONFIG_CONSTANTS.POLL_MAX_TIMES,
  pollInterval: CONFIG_CONSTANTS.POLL_INTERVAL_MS,

  // 超时配置
  taskTimeout: CONFIG_CONSTANTS.TASK_TIMEOUT_DEFAULT,

  // 重试配置
  retryBaseDelay: CONFIG_CONSTANTS.RETRY_BASE_DELAY,

  // 用户模型获取
  getUserModel: () => {
    throw new Error('getUserModel 必须在配置中提供');
  },

  // 认证获取
  getAuthenticate: () => {
    throw new Error('getAuthenticate 必须在配置中提供');
  },

  // 任务处理器
  task: {},

  // 任务错误统一处理（可选）
  // 当任务执行失败时调用，接收参数: { task, error, type }
  // - task: 任务对象
  // - error: 错误信息
  // - type: 错误类型（execution/timeout/callback/retry_exhausted）
  errorHandler: null,

  // 统计配置
  statistics: {
    enabled: true,
    retentionDays: CONFIG_CONSTANTS.LOGS_RETENTION_DAYS
  },

  // 安全配置
  security: {
    enableSignature: true,
    enablePathValidation: true,
    enableInputValidation: true,
    maxTaskSize: 1024 * 1024 // 1MB
  }
};

/**
 * 配置管理器
 */
class ConfigManager {
  constructor(userConfig = {}) {
    this.config = this.mergeConfig(DEFAULT_CONFIG, userConfig);
    this.validateConfig();
  }

  /**
   * 合并配置
   * @param {Object} defaults - 默认配置
   * @param {Object} userConfig - 用户配置
   * @returns {Object} 合并后的配置
   */
  mergeConfig(defaults, userConfig) {
    // 深拷贝默认配置的嵌套对象，避免修改 DEFAULT_CONFIG
    const merged = {};
    Object.keys(defaults).forEach(key => {
      const val = defaults[key];
      if (typeof val === 'object' && !Array.isArray(val) && val !== null) {
        merged[key] = { ...val };
      } else if (Array.isArray(val)) {
        merged[key] = [...val];
      } else {
        merged[key] = val;
      }
    });

    // 处理 dirs 配置的向后兼容
    if (!userConfig.dirs) {
      merged.dirs = [merged.dir];
    } else if (!userConfig.dirs.includes(merged.dir)) {
      merged.dirs = [merged.dir, ...userConfig.dirs];
    } else {
      merged.dirs = [...userConfig.dirs];
    }

    // 合并其他配置
    Object.keys(userConfig).forEach(key => {
      if (key !== 'dirs') {
        if (typeof userConfig[key] === 'object' && !Array.isArray(userConfig[key]) && userConfig[key] !== null) {
          merged[key] = { ...defaults[key], ...userConfig[key] };
        } else {
          merged[key] = userConfig[key];
        }
      }
    });

    return merged;
  }

  /**
   * 验证配置
   * @throws {Error} 如果配置无效
   */
  validateConfig() {
    // 验证必需的配置项
    if (!this.config.name || typeof this.config.name !== 'string') {
      throw new Error('配置项 name 必须为非空字符串');
    }

    // 验证数值范围
    if (this.config.limit < CONFIG_CONSTANTS.CONCURRENT_TASKS_MIN || this.config.limit > CONFIG_CONSTANTS.CONCURRENT_TASKS_MAX) {
      throw new Error(`配置项 limit 必须在 ${CONFIG_CONSTANTS.CONCURRENT_TASKS_MIN} 到 ${CONFIG_CONSTANTS.CONCURRENT_TASKS_MAX} 之间`);
    }

    if (this.config.taskTimeout < CONFIG_CONSTANTS.TASK_TIMEOUT_MIN || this.config.taskTimeout > CONFIG_CONSTANTS.TASK_TIMEOUT_MAX) {
      throw new Error(`配置项 taskTimeout 必须在 ${CONFIG_CONSTANTS.TASK_TIMEOUT_MIN} 到 ${CONFIG_CONSTANTS.TASK_TIMEOUT_MAX} 之间`);
    }

    // 验证目录配置
    if (!Array.isArray(this.config.dirs) || this.config.dirs.length === 0) {
      throw new Error('配置项 dirs 必须为非空数组');
    }

    // 验证函数类型
    if (typeof this.config.getUserModel !== 'function') {
      throw new Error('配置项 getUserModel 必须为函数');
    }

    if (typeof this.config.getAuthenticate !== 'function') {
      throw new Error('配置项 getAuthenticate 必须为函数');
    }

    // 验证 errorHandler（可选，如果提供则必须为函数）
    if (this.config.errorHandler !== null && typeof this.config.errorHandler !== 'function') {
      throw new Error('配置项 errorHandler 必须为函数或 null');
    }
  }

  /**
   * 获取配置值
   * @param {string} key - 配置键
   * @param {*} defaultValue - 默认值
   * @returns {*} 配置值
   */
  get(key, defaultValue = undefined) {
    const keys = key.split('.');
    let value = this.config;

    for (const k of keys) {
      if (value && typeof value === 'object' && k in value) {
        value = value[k];
      } else {
        return defaultValue;
      }
    }

    return value;
  }

  /**
   * 设置配置值
   * @param {string} key - 配置键
   * @param {*} value - 配置值
   */
  set(key, value) {
    const keys = key.split('.');
    let target = this.config;

    for (let i = 0; i < keys.length - 1; i++) {
      const k = keys[i];
      if (!(k in target) || typeof target[k] !== 'object') {
        target[k] = {};
      }
      target = target[k];
    }

    target[keys[keys.length - 1]] = value;
  }

  /**
   * 获取完整配置
   * @returns {Object} 完整配置对象
   */
  getAll() {
    return { ...this.config };
  }

  /**
   * 获取常量
   * @returns {Object} 常量对象
   */
  getConstants() {
    return { ...CONFIG_CONSTANTS };
  }
}

// 创建单例实例
let configManagerInstance = null;

/**
 * 获取配置管理器实例
 * @param {Object} userConfig - 用户配置
 * @returns {ConfigManager} 配置管理器实例
 */
function getConfigManager(userConfig = null) {
  if (userConfig) {
    configManagerInstance = new ConfigManager(userConfig);
  }
  return configManagerInstance;
}

// 测试用：重置单例
function _resetConfigManager() {
  configManagerInstance = null;
}

module.exports = {
  CONFIG_CONSTANTS,
  DEFAULT_CONFIG,
  ConfigManager,
  getConfigManager,
  _resetConfigManager
};

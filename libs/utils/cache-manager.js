/**
 * 内存缓存管理器
 * 提供简单的缓存功能，支持过期时间
 */
class CacheManager {
  constructor(defaultTTL = 300000) { // 默认5分钟过期
    this.cache = new Map();
    this.defaultTTL = defaultTTL;
    this.timers = new Map();
  }

  /**
   * 设置缓存
   * @param {string} key - 缓存键
   * @param {*} value - 缓存值
   * @param {number} ttl - 过期时间（毫秒）
   */
  set(key, value, ttl = this.defaultTTL) {
    // 清除现有的定时器
    if (this.timers.has(key)) {
      clearTimeout(this.timers.get(key));
    }

    // 设置缓存
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + ttl
    });

    // 设置过期定时器
    const timer = setTimeout(() => {
      this.delete(key);
    }, ttl);
    timer.unref();

    this.timers.set(key, timer);
  }

  /**
   * 获取缓存
   * @param {string} key - 缓存键
   * @returns {*} 缓存值，如果不存在或已过期返回 null
   */
  get(key) {
    const item = this.cache.get(key);

    if (!item) {
      return null;
    }

    // 检查是否过期
    if (Date.now() > item.expiresAt) {
      this.delete(key);
      return null;
    }

    return item.value;
  }

  /**
   * 删除缓存
   * @param {string} key - 缓存键
   */
  delete(key) {
    if (this.timers.has(key)) {
      clearTimeout(this.timers.get(key));
      this.timers.delete(key);
    }
    this.cache.delete(key);
  }

  /**
   * 清空所有缓存
   */
  clear() {
    // 清除所有定时器
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    this.cache.clear();
  }

  /**
   * 检查缓存是否存在且未过期
   * @param {string} key - 缓存键
   * @returns {boolean} 是否存在有效缓存
   */
  has(key) {
    return this.get(key) !== null;
  }

  /**
   * 获取或设置缓存
   * @param {string} key - 缓存键
   * @param {Function} factory - 值工厂函数
   * @param {number} ttl - 过期时间（毫秒）
   * @returns {*} 缓存值
   */
  async getOrSet(key, factory, ttl = this.defaultTTL) {
    const cached = this.get(key);
    if (cached !== null) {
      return cached;
    }

    const value = await factory();
    this.set(key, value, ttl);
    return value;
  }

  /**
   * 清理过期缓存
   */
  cleanup() {
    const now = Date.now();
    for (const [key, item] of this.cache.entries()) {
      if (now > item.expiresAt) {
        this.delete(key);
      }
    }
  }

  /**
   * 获取缓存统计信息
   * @returns {Object} 统计信息
   */
  getStats() {
    return {
      size: this.cache.size,
      timerCount: this.timers.size
    };
  }
}

// 创建单例实例
let cacheManagerInstance = null;

/**
 * 获取缓存管理器实例
 * @param {number} defaultTTL - 默认过期时间
 * @returns {CacheManager} 缓存管理器实例
 */
function getCacheManager(defaultTTL = null) {
  if (defaultTTL !== null || !cacheManagerInstance) {
    cacheManagerInstance = new CacheManager(defaultTTL);
  }
  return cacheManagerInstance;
}

// 测试用：重置单例
function _resetCacheManager() {
  if (cacheManagerInstance) {
    cacheManagerInstance.clear();
  }
  cacheManagerInstance = null;
}

module.exports = {
  CacheManager,
  getCacheManager,
  _resetCacheManager
};
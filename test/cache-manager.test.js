const { expect } = require('chai');
const { CacheManager, getCacheManager, _resetCacheManager } = require('../libs/utils/cache-manager');

describe('cache-manager.js', () => {
  let cache;

  beforeEach(() => {
    cache = new CacheManager(60000);
  });

  afterEach(() => {
    cache.clear();
  });

  describe('set / get', () => {
    it('should set and get a value', () => {
      cache.set('key1', 'value1');
      expect(cache.get('key1')).to.equal('value1');
    });

    it('should return null for missing key', () => {
      expect(cache.get('nonexistent')).to.equal(null);
    });

    it('should overwrite existing value and clear old timer', () => {
      cache.set('key1', 'val1', 10000);
      cache.set('key1', 'val2', 10000);
      expect(cache.get('key1')).to.equal('val2');
    });
  });

  describe('get with expired entry', () => {
    it('should return null and delete expired entry', () => {
      cache.set('key1', 'val1', 1);
      // Manually force expiresAt into the past
      const item = cache.cache.get('key1');
      item.expiresAt = Date.now() - 1000;
      expect(cache.get('key1')).to.equal(null);
      expect(cache.cache.has('key1')).to.equal(false);
    });
  });

  describe('delete', () => {
    it('should delete entry and clear its timer', () => {
      cache.set('key1', 'val1', 10000);
      expect(cache.timers.has('key1')).to.equal(true);
      cache.delete('key1');
      expect(cache.get('key1')).to.equal(null);
      expect(cache.timers.has('key1')).to.equal(false);
    });

    it('should handle deleting non-existent key', () => {
      expect(() => cache.delete('nonexistent')).to.not.throw();
    });
  });

  describe('clear', () => {
    it('should clear all entries and timers', () => {
      cache.set('a', 1, 10000);
      cache.set('b', 2, 10000);
      cache.clear();
      expect(cache.get('a')).to.equal(null);
      expect(cache.get('b')).to.equal(null);
      expect(cache.timers.size).to.equal(0);
      expect(cache.cache.size).to.equal(0);
    });
  });

  describe('has', () => {
    it('should return true for existing non-expired entry', () => {
      cache.set('key1', 'val1');
      expect(cache.has('key1')).to.equal(true);
    });

    it('should return false for missing key', () => {
      expect(cache.has('nonexistent')).to.equal(false);
    });
  });

  describe('getOrSet', () => {
    it('should return cached value if exists', async () => {
      cache.set('key1', 'cached');
      const result = await cache.getOrSet('key1', async () => 'new');
      expect(result).to.equal('cached');
    });

    it('should call factory and cache result if not exists', async () => {
      const result = await cache.getOrSet('key1', async () => 'computed', 10000);
      expect(result).to.equal('computed');
      expect(cache.get('key1')).to.equal('computed');
    });
  });

  describe('cleanup', () => {
    it('should remove expired entries', () => {
      cache.set('key1', 'val1', 1);
      cache.set('key2', 'val2', 60000);
      // Force key1 to be expired
      const item = cache.cache.get('key1');
      item.expiresAt = Date.now() - 1000;
      cache.cleanup();
      expect(cache.cache.has('key1')).to.equal(false);
      expect(cache.cache.has('key2')).to.equal(true);
    });
  });

  describe('getStats', () => {
    it('should return cache statistics', () => {
      cache.set('a', 1, 10000);
      cache.set('b', 2, 10000);
      const stats = cache.getStats();
      expect(stats.size).to.equal(2);
      expect(stats.timerCount).to.equal(2);
    });
  });

  describe('getCacheManager singleton', () => {
    it('should return same instance when no TTL provided', () => {
      const inst1 = getCacheManager();
      const inst2 = getCacheManager();
      expect(inst1).to.equal(inst2);
    });

    it('should create new instance when TTL provided', () => {
      const inst1 = getCacheManager();
      const inst2 = getCacheManager(120000);
      expect(inst2).to.not.equal(inst1);
    });
  });

  describe('_resetCacheManager', () => {
    it('should reset singleton instance', () => {
      const inst = getCacheManager(200000);
      _resetCacheManager();
      const inst2 = getCacheManager(200000);
      expect(inst2).to.not.equal(inst);
    });

    it('should handle resetting when no instance exists', () => {
      _resetCacheManager();
      expect(() => _resetCacheManager()).to.not.throw();
    });
  });
});

const { expect } = require('chai');
const SecurityValidator = require('../libs/utils/security-validator');

describe('security-validator.js', () => {
  describe('validateScriptPath', () => {
    it('should throw when baseDir is empty', () => {
      expect(() => SecurityValidator.validateScriptPath('', 'test.js')).to.throw('路径参数不能为空');
    });

    it('should throw when requestedPath is empty', () => {
      expect(() => SecurityValidator.validateScriptPath('/base', '')).to.throw('路径参数不能为空');
    });

    it('should throw for path traversal', () => {
      expect(() => SecurityValidator.validateScriptPath('/base', '../etc/passwd.js')).to.throw('非法路径访问');
    });

    it('should throw for non-.js extension', () => {
      expect(() => SecurityValidator.validateScriptPath('/base', 'test.txt')).to.throw('只允许 .js 文件');
    });

    it('should throw for suspicious characters', () => {
      expect(() => SecurityValidator.validateScriptPath('/base', 'test~.js')).to.throw('路径包含非法字符');
    });

    it('should return resolved path for valid input', () => {
      const result = SecurityValidator.validateScriptPath('/base', 'test.js');
      expect(result).to.include('test.js');
    });
  });

  describe('validateTaskType', () => {
    it('should throw for empty task type', () => {
      expect(() => SecurityValidator.validateTaskType('')).to.throw('任务类型必须为非空字符串');
    });

    it('should throw for non-string task type', () => {
      expect(() => SecurityValidator.validateTaskType(123)).to.throw('任务类型必须为非空字符串');
    });

    it('should throw for invalid format', () => {
      expect(() => SecurityValidator.validateTaskType('bad type!')).to.throw('任务类型格式无效');
    });

    it('should throw for disallowed type', () => {
      expect(() => SecurityValidator.validateTaskType('unknown', ['allowed'])).to.throw('不支持的任务类型');
    });

    it('should return valid task type', () => {
      expect(SecurityValidator.validateTaskType('my-type')).to.equal('my-type');
    });

    it('should allow type in allowed list', () => {
      expect(SecurityValidator.validateTaskType('allowed', ['allowed'])).to.equal('allowed');
    });
  });

  describe('validateNumber', () => {
    it('should throw when value is null and required', () => {
      expect(() => SecurityValidator.validateNumber(null, { required: true })).to.throw('数值参数不能为空');
    });

    it('should return defaultValue when value is null and not required', () => {
      const result = SecurityValidator.validateNumber(null, { required: false, defaultValue: 42 });
      expect(result).to.equal(42);
    });

    it('should throw when value is NaN', () => {
      expect(() => SecurityValidator.validateNumber('abc')).to.throw('参数必须为有效数字');
    });

    it('should throw when integer is required but value is float', () => {
      expect(() => SecurityValidator.validateNumber(1.5, { integer: true })).to.throw('参数必须为整数');
    });

    it('should return numValue on valid input', () => {
      expect(SecurityValidator.validateNumber(5, { min: 0, max: 10 })).to.equal(5);
    });

    it('should allow float when integer is false', () => {
      expect(SecurityValidator.validateNumber(1.5, { integer: false })).to.equal(1.5);
    });

    it('should throw when value out of range', () => {
      expect(() => SecurityValidator.validateNumber(11, { min: 0, max: 10 })).to.throw('参数必须在 0 到 10 之间');
    });

    it('should throw when value below min', () => {
      expect(() => SecurityValidator.validateNumber(-1, { min: 0 })).to.throw('参数必须在 0 到');
    });
  });

  describe('generateSignature', () => {
    it('should throw when secret is empty', () => {
      expect(() => SecurityValidator.generateSignature({ secret: '', id: '1', data: {} })).to.throw('签名密钥不能为空');
    });

    it('should throw when id is empty', () => {
      expect(() => SecurityValidator.generateSignature({ secret: 's', id: '', data: {} })).to.throw('签名ID不能为空');
    });

    it('should generate a hex signature', () => {
      const sig = SecurityValidator.generateSignature({ secret: 's', id: '1', data: { a: 1 } });
      expect(sig).to.match(/^[0-9a-f]+$/);
    });

    it('should handle string data', () => {
      const sig = SecurityValidator.generateSignature({ secret: 's', id: '1', data: 'hello' });
      expect(sig).to.match(/^[0-9a-f]+$/);
    });
  });

  describe('verifySignature', () => {
    it('should return false when signature is empty', () => {
      expect(SecurityValidator.verifySignature({ secret: 's', id: '1', data: {}, signature: '' })).to.equal(false);
    });

    it('should return true when secret is empty (backward compat)', () => {
      expect(SecurityValidator.verifySignature({ secret: '', id: '1', data: {}, signature: 'sig' })).to.equal(true);
    });

    it('should return true for valid signature', () => {
      const sig = SecurityValidator.generateSignature({ secret: 's', id: '1', data: {} });
      expect(SecurityValidator.verifySignature({ secret: 's', id: '1', data: {}, signature: sig })).to.equal(true);
    });

    it('should return false for invalid signature', () => {
      expect(SecurityValidator.verifySignature({ secret: 's', id: '1', data: {}, signature: 'bad' })).to.equal(false);
    });

    it('should return false on exception', () => {
      const circular = {};
      circular.self = circular;
      expect(SecurityValidator.verifySignature({ secret: 's', id: '1', data: circular, signature: 'x' })).to.equal(false);
    });
  });

  describe('sanitizeStackTrace', () => {
    it('should return empty string when no error', () => {
      expect(SecurityValidator.sanitizeStackTrace(null)).to.equal('');
    });

    it('should return empty string when no stack', () => {
      expect(SecurityValidator.sanitizeStackTrace({})).to.equal('');
    });

    it('should replace cwd with /server', () => {
      const err = new Error('test');
      const cwd = process.cwd();
      if (err.stack.includes(cwd)) {
        const result = SecurityValidator.sanitizeStackTrace(err, cwd);
        expect(result).to.include('/server');
        expect(result).to.not.include(cwd);
      } else {
        // stack may not contain cwd in some environments
        const result = SecurityValidator.sanitizeStackTrace(err, cwd);
        expect(result).to.be.a('string');
      }
    });
  });

  describe('safeJSONParse', () => {
    it('should return object as-is', () => {
      const obj = { key: 'val' };
      expect(SecurityValidator.safeJSONParse(obj)).to.equal(obj);
    });

    it('should parse valid JSON string', () => {
      expect(SecurityValidator.safeJSONParse('{"a":1}')).to.deep.equal({ a: 1 });
    });

    it('should return defaultValue on invalid JSON', () => {
      expect(SecurityValidator.safeJSONParse('not-json', 'fallback')).to.equal('fallback');
    });

    it('should return null by default on invalid JSON', () => {
      expect(SecurityValidator.safeJSONParse('{bad}')).to.equal(null);
    });
  });

  describe('limitLogsSize', () => {
    it('should return empty array for non-array input', () => {
      expect(SecurityValidator.limitLogsSize(null)).to.deep.equal([]);
      expect(SecurityValidator.limitLogsSize('not-array')).to.deep.equal([]);
    });

    it('should return logs as-is when within limit', () => {
      const logs = [1, 2, 3];
      expect(SecurityValidator.limitLogsSize(logs, 10)).to.deep.equal([1, 2, 3]);
    });

    it('should truncate to latest maxSize entries', () => {
      const logs = Array.from({ length: 150 }, (_, i) => i);
      const result = SecurityValidator.limitLogsSize(logs, 100);
      expect(result).to.have.length(100);
      expect(result[0]).to.equal(50);
      expect(result[99]).to.equal(149);
    });
  });
});

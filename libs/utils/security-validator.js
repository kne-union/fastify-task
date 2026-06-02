const path = require('path');
const crypto = require('node:crypto');

/**
 * 安全验证工具类
 * 用于路径验证、输入验证、签名验证等安全检查
 */
class SecurityValidator {
  /**
   * 验证脚本路径是否安全，防止路径遍历攻击
   * @param {string} baseDir - 基础目录
   * @param {string} requestedPath - 请求的路径
   * @returns {string} 解析后的安全路径
   * @throws {Error} 如果路径不安全
   */
  static validateScriptPath(baseDir, requestedPath) {
    if (!baseDir || !requestedPath) {
      throw new Error('路径参数不能为空');
    }

    // 解析为绝对路径
    const resolvedBaseDir = path.resolve(baseDir);
    const resolvedPath = path.resolve(baseDir, requestedPath);

    // 检查是否在基础目录内
    if (!resolvedPath.startsWith(resolvedBaseDir)) {
      throw new Error(`非法路径访问: ${requestedPath}`);
    }

    // 检查文件扩展名
    const ext = path.extname(resolvedPath);
    if (ext !== '.js') {
      throw new Error(`只允许 .js 文件: ${requestedPath}`);
    }

    // 检查路径中是否包含可疑字符
    const suspiciousChars = ['..', '~', '$'];
    for (const char of suspiciousChars) {
      if (requestedPath.includes(char)) {
        throw new Error(`路径包含非法字符: ${char}`);
      }
    }

    return resolvedPath;
  }

  /**
   * 验证任务类型是否安全
   * @param {string} taskType - 任务类型
   * @param {Array} allowedTypes - 允许的任务类型列表
   * @throws {Error} 如果任务类型不安全
   */
  static validateTaskType(taskType, allowedTypes = []) {
    if (!taskType || typeof taskType !== 'string') {
      throw new Error('任务类型必须为非空字符串');
    }

    // 检查任务类型格式
    if (!/^[a-zA-Z0-9_-]+$/.test(taskType)) {
      throw new Error(`任务类型格式无效: ${taskType}`);
    }

    // 如果有允许列表，检查是否在列表中
    if (allowedTypes.length > 0 && !allowedTypes.includes(taskType)) {
      throw new Error(`不支持的任务类型: ${taskType}`);
    }

    return taskType;
  }

  /**
   * 验证数值参数
   * @param {number} value - 要验证的值
   * @param {Object} options - 验证选项
   * @returns {number} 验证后的值
   * @throws {Error} 如果验证失败
   */
  static validateNumber(value, options = {}) {
    const {
      min = 0,
      max = Number.MAX_SAFE_INTEGER,
      integer = true,
      required = true,
      defaultValue = 0
    } = options;

    if (value === undefined || value === null) {
      if (required) {
        throw new Error('数值参数不能为空');
      }
      return defaultValue;
    }

    const numValue = Number(value);

    if (isNaN(numValue)) {
      throw new Error('参数必须为有效数字');
    }

    if (integer && !Number.isInteger(numValue)) {
      throw new Error('参数必须为整数');
    }

    if (numValue < min || numValue > max) {
      throw new Error(`参数必须在 ${min} 到 ${max} 之间`);
    }

    return numValue;
  }

  /**
   * 生成HMAC-SHA256签名
   * @param {Object} params - 签名参数
   * @returns {string} 签名结果
   */
  static generateSignature({ secret, id, data }) {
    if (!secret) {
      throw new Error('签名密钥不能为空');
    }
    if (!id) {
      throw new Error('签名ID不能为空');
    }

    const dataToSign = `${id}|${typeof data === 'string' ? data : JSON.stringify(data)}`;
    const hmac = crypto.createHmac('sha256', secret);
    hmac.update(dataToSign);
    return hmac.digest('hex');
  }

  /**
   * 验证HMAC-SHA256签名
   * @param {Object} params - 验证参数
   * @returns {boolean} 验证结果
   */
  static verifySignature({ secret, id, data, signature }) {
    try {
      if (!signature) {
        return false;
      }

      if (!secret) {
        // 如果没有配置密钥，则跳过签名验证（向后兼容）
        return true;
      }

      const expectedSignature = this.generateSignature({ secret, id, data });
      return signature === expectedSignature;
    } catch (error) {
      return false;
    }
  }

  /**
   * 净化错误堆栈信息，防止敏感信息泄露
   * @param {Error} error - 错误对象
   * @param {string} cwd - 当前工作目录
   * @returns {string} 净化后的堆栈信息
   */
  static sanitizeStackTrace(error, cwd = process.cwd()) {
    if (!error || !error.stack) {
      return '';
    }
    return error.stack.replaceAll(cwd, '/server');
  }

  /**
   * 验证JSON数据
   * @param {string} jsonString - JSON字符串
   * @param {*} defaultValue - 默认值
   * @returns {*} 解析后的JSON对象
   */
  static safeJSONParse(jsonString, defaultValue = null) {
    try {
      if (typeof jsonString === 'object') {
        return jsonString;
      }
      return JSON.parse(jsonString);
    } catch (error) {
      return defaultValue;
    }
  }

  /**
   * 验证并限制日志大小
   * @param {Array} logs - 日志数组
   * @param {number} maxSize - 最大日志数量
   * @returns {Array} 限制后的日志数组
   */
  static limitLogsSize(logs, maxSize = 100) {
    if (!Array.isArray(logs)) {
      return [];
    }

    if (logs.length <= maxSize) {
      return logs;
    }

    // 保留最新的 maxSize 条日志
    return logs.slice(-maxSize);
  }
}

module.exports = SecurityValidator;
/**
 * 自定义错误类体系
 * 提供结构化的错误处理机制
 */

// 基础任务错误类
class TaskError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.name = 'TaskError';
    this.code = code;
    this.details = details;
    this.timestamp = new Date().toISOString();
    Error.captureStackTrace(this, this.constructor);
  }

  toJSON() {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      details: this.details,
      timestamp: this.timestamp
    };
  }
}

// 任务不存在错误
class TaskNotFoundError extends TaskError {
  constructor(taskId, details = {}) {
    super('TASK_NOT_FOUND', `任务不存在: ${taskId}`, { taskId, ...details });
    this.name = 'TaskNotFoundError';
  }
}

// 任务状态错误
class TaskStatusError extends TaskError {
  constructor(taskId, currentStatus, expectedStatus, details = {}) {
    super(
      'TASK_STATUS_ERROR',
      `任务状态不允许此操作: 当前状态 ${currentStatus}, 期望状态 ${expectedStatus}`,
      { taskId, currentStatus, expectedStatus, ...details }
    );
    this.name = 'TaskStatusError';
  }
}

// 任务超时错误
class TaskTimeoutError extends TaskError {
  constructor(taskId, timeout, details = {}) {
    super('TASK_TIMEOUT', `任务执行超时: ${timeout}ms`, { taskId, timeout, ...details });
    this.name = 'TaskTimeoutError';
  }
}

// 任务验证错误
class TaskValidationError extends TaskError {
  constructor(field, value, reason, details = {}) {
    super(
      'TASK_VALIDATION_ERROR',
      `任务参数验证失败: ${field} = ${value}, 原因: ${reason}`,
      { field, value, reason, ...details }
    );
    this.name = 'TaskValidationError';
  }
}

// 任务执行错误
class TaskExecutionError extends TaskError {
  constructor(taskId, originalError, details = {}) {
    super(
      'TASK_EXECUTION_ERROR',
      `任务执行失败: ${originalError.message}`,
      { taskId, originalError: originalError.message, ...details }
    );
    this.name = 'TaskExecutionError';
    this.originalError = originalError;
  }
}

// 安全错误
class SecurityError extends TaskError {
  constructor(reason, details = {}) {
    super('SECURITY_ERROR', `安全检查失败: ${reason}`, { reason, ...details });
    this.name = 'SecurityError';
  }
}

// 配置错误
class ConfigurationError extends TaskError {
  constructor(configKey, reason, details = {}) {
    super('CONFIGURATION_ERROR', `配置错误: ${configKey} - ${reason}`, { configKey, reason, ...details });
    this.name = 'ConfigurationError';
  }
}

// 数据库错误
class DatabaseError extends TaskError {
  constructor(operation, originalError, details = {}) {
    super(
      'DATABASE_ERROR',
      `数据库操作失败: ${operation}`,
      { operation, originalError: originalError.message, ...details }
    );
    this.name = 'DatabaseError';
    this.originalError = originalError;
  }
}

// 错误处理工具函数
class ErrorHandler {
  /**
   * 处理错误并返回适当的响应
   * @param {Error} error - 错误对象
   * @param {Object} logger - 日志记录器
   * @returns {Object} 错误响应
   */
  static handle(error, logger) {
    if (error instanceof TaskError) {
      // 自定义任务错误
      logger.warn(`任务错误 [${error.code}]: ${error.message}`, error.details);
      return {
        success: false,
        error: {
          code: error.code,
          message: error.message,
          details: error.details
        }
      };
    }

    // 未知错误
    logger.error('未知错误:', error);
    return {
      success: false,
      error: {
        code: 'UNKNOWN_ERROR',
        message: '服务器内部错误',
        details: process.env.NODE_ENV === 'development' ? { message: error.message } : {}
      }
    };
  }

  /**
   * 异步包装器，自动处理错误
   * @param {Function} fn - 异步函数
   * @param {Object} logger - 日志记录器
   * @returns {Function} 包装后的函数
   */
  static asyncWrapper(fn, logger) {
    return async (...args) => {
      try {
        return await fn(...args);
      } catch (error) {
        throw this.handle(error, logger);
      }
    };
  }

  /**
   * 将错误转换为任务错误
   * @param {Error} error - 原始错误
   * @param {string} defaultCode - 默认错误代码
   * @param {string} defaultMessage - 默认错误消息
   * @returns {TaskError} 任务错误对象
   */
  static toTaskError(error, defaultCode = 'UNKNOWN_ERROR', defaultMessage = '未知错误') {
    if (error instanceof TaskError) {
      return error;
    }

    return new TaskError(
      defaultCode,
      defaultMessage,
      { originalError: error.message, stack: error.stack }
    );
  }
}

module.exports = {
  TaskError,
  TaskNotFoundError,
  TaskStatusError,
  TaskTimeoutError,
  TaskValidationError,
  TaskExecutionError,
  SecurityError,
  ConfigurationError,
  DatabaseError,
  ErrorHandler
};
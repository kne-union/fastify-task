const { expect } = require('chai');
const {
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
} = require('../libs/utils/errors');

describe('errors.js', () => {
  describe('TaskError', () => {
    it('should create TaskError with code, message, details', () => {
      const err = new TaskError('TEST_CODE', 'test message', { foo: 'bar' });
      expect(err.name).to.equal('TaskError');
      expect(err.code).to.equal('TEST_CODE');
      expect(err.message).to.equal('test message');
      expect(err.details).to.deep.equal({ foo: 'bar' });
      expect(err.timestamp).to.be.a('string');
    });

    it('toJSON should return serializable object', () => {
      const err = new TaskError('CODE', 'msg', { key: 'val' });
      const json = err.toJSON();
      expect(json).to.deep.include({
        name: 'TaskError',
        code: 'CODE',
        message: 'msg',
        details: { key: 'val' }
      });
      expect(json.timestamp).to.be.a('string');
    });
  });

  describe('TaskNotFoundError', () => {
    it('should create with taskId', () => {
      const err = new TaskNotFoundError('task-1');
      expect(err.name).to.equal('TaskNotFoundError');
      expect(err.code).to.equal('TASK_NOT_FOUND');
      expect(err.message).to.include('task-1');
      expect(err.details.taskId).to.equal('task-1');
    });
  });

  describe('TaskStatusError', () => {
    it('should create with taskId, currentStatus, expectedStatus', () => {
      const err = new TaskStatusError('task-1', 'running', 'pending');
      expect(err.name).to.equal('TaskStatusError');
      expect(err.code).to.equal('TASK_STATUS_ERROR');
      expect(err.details.currentStatus).to.equal('running');
      expect(err.details.expectedStatus).to.equal('pending');
    });
  });

  describe('TaskTimeoutError', () => {
    it('should create with taskId and timeout', () => {
      const err = new TaskTimeoutError('task-1', 30000);
      expect(err.name).to.equal('TaskTimeoutError');
      expect(err.code).to.equal('TASK_TIMEOUT');
      expect(err.details.timeout).to.equal(30000);
    });
  });

  describe('TaskValidationError', () => {
    it('should create with field, value, reason', () => {
      const err = new TaskValidationError('priority', 'high', 'must be number');
      expect(err.name).to.equal('TaskValidationError');
      expect(err.code).to.equal('TASK_VALIDATION_ERROR');
      expect(err.details.field).to.equal('priority');
    });
  });

  describe('TaskExecutionError', () => {
    it('should create with taskId and originalError', () => {
      const original = new Error('db down');
      const err = new TaskExecutionError('task-1', original, { operation: 'create' });
      expect(err.name).to.equal('TaskExecutionError');
      expect(err.code).to.equal('TASK_EXECUTION_ERROR');
      expect(err.originalError).to.equal(original);
      expect(err.details.operation).to.equal('create');
    });
  });

  describe('SecurityError', () => {
    it('should create with reason', () => {
      const err = new SecurityError('invalid signature');
      expect(err.name).to.equal('SecurityError');
      expect(err.code).to.equal('SECURITY_ERROR');
      expect(err.details.reason).to.equal('invalid signature');
    });
  });

  describe('ConfigurationError', () => {
    it('should create with configKey and reason', () => {
      const err = new ConfigurationError('limit', 'out of range');
      expect(err.name).to.equal('ConfigurationError');
      expect(err.code).to.equal('CONFIGURATION_ERROR');
      expect(err.details.configKey).to.equal('limit');
      expect(err.details.reason).to.equal('out of range');
    });
  });

  describe('DatabaseError', () => {
    it('should create with operation and originalError', () => {
      const original = new Error('connection lost');
      const err = new DatabaseError('insert', original);
      expect(err.name).to.equal('DatabaseError');
      expect(err.code).to.equal('DATABASE_ERROR');
      expect(err.details.operation).to.equal('insert');
      expect(err.originalError).to.equal(original);
    });
  });

  describe('ErrorHandler.handle', () => {
    it('should handle TaskError and return structured response', () => {
      const logger = { warn: () => {}, error: () => {} };
      const err = new TaskValidationError('field', 'val', 'bad');
      const result = ErrorHandler.handle(err, logger);
      expect(result.success).to.equal(false);
      expect(result.error.code).to.equal('TASK_VALIDATION_ERROR');
      expect(result.error.message).to.be.a('string');
      expect(result.error.details).to.exist;
    });

    it('should handle unknown error and return UNKNOWN_ERROR', () => {
      const logger = { warn: () => {}, error: () => {} };
      const err = new Error('something went wrong');
      const result = ErrorHandler.handle(err, logger);
      expect(result.success).to.equal(false);
      expect(result.error.code).to.equal('UNKNOWN_ERROR');
      expect(result.error.message).to.equal('服务器内部错误');
    });

    it('should include error message in development mode', () => {
      const origEnv = process.env.NODE_ENV;
      process.env.NODE_ENV = 'development';
      const logger = { warn: () => {}, error: () => {} };
      const err = new Error('dev error');
      const result = ErrorHandler.handle(err, logger);
      expect(result.error.details.message).to.equal('dev error');
      process.env.NODE_ENV = origEnv;
    });
  });

  describe('ErrorHandler.asyncWrapper', () => {
    it('should return result on success', async () => {
      const logger = { warn: () => {}, error: () => {} };
      const fn = async () => 42;
      const wrapped = ErrorHandler.asyncWrapper(fn, logger);
      const result = await wrapped();
      expect(result).to.equal(42);
    });

    it('should throw handled error on failure', async () => {
      const logger = { warn: () => {}, error: () => {} };
      const fn = async () => { throw new Error('boom'); };
      const wrapped = ErrorHandler.asyncWrapper(fn, logger);
      try {
        await wrapped();
        throw new Error('Should have thrown');
      } catch (e) {
        // handleError returns an object with success:false
        expect(e.success).to.equal(false);
        expect(e.error.code).to.equal('UNKNOWN_ERROR');
      }
    });
  });

  describe('ErrorHandler.toTaskError', () => {
    it('should return TaskError as-is', () => {
      const err = new TaskValidationError('f', 'v', 'r');
      const result = ErrorHandler.toTaskError(err);
      expect(result).to.equal(err);
    });

    it('should wrap plain Error into TaskError', () => {
      const err = new Error('plain error');
      const result = ErrorHandler.toTaskError(err, 'CUSTOM_CODE', 'Custom message');
      expect(result).to.be.instanceOf(TaskError);
      expect(result.code).to.equal('CUSTOM_CODE');
      expect(result.message).to.equal('Custom message');
      expect(result.details.originalError).to.equal('plain error');
    });

    it('should use default code/message when not provided', () => {
      const err = new Error('x');
      const result = ErrorHandler.toTaskError(err);
      expect(result.code).to.equal('UNKNOWN_ERROR');
      expect(result.message).to.equal('未知错误');
    });
  });
});

const { expect } = require('chai');
const sinon = require('sinon');
const crypto = require('node:crypto');
const { createFastify } = require('./helpers/setup');

describe('@kne/fastify-task - 服务功能', function () {
  this.timeout(10000);

  let fastify;
  let taskData = [];
  let taskIdCounter = { value: 1 };

  beforeEach(() => {
    taskData = [];
    taskIdCounter.value = 1;
  });

  afterEach(async () => {
    if (fastify) {
      await fastify.close();
      fastify = null;
    }
    sinon.restore();
  });

  describe('插件注册测试', () => {
    it('should register plugin with default options', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      expect(fastify.task).to.exist;
      expect(fastify.task.services).to.exist;
      expect(fastify.task.models).to.exist;
    });

    it('should register plugin with custom options', async () => {
      fastify = await createFastify({ name: 'task', limit: 5 }, taskData, taskIdCounter);
      await fastify.ready();
      expect(fastify.task).to.exist;
      expect(fastify.task.options.limit).to.equal(5);
    });

    it('should expose all required services', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const services = fastify.task.services;
      expect(services.create).to.exist;
      expect(services.detail).to.exist;
      expect(services.list).to.exist;
      expect(services.complete).to.exist;
      expect(services.cancel).to.exist;
      expect(services.runner).to.exist;
      expect(services.resetAll).to.exist;
      expect(services.retry).to.exist;
      expect(services.log).to.exist;
      expect(services.callback).to.exist;
    });

    it('should register all API routes', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const listResponse = await fastify.inject({ method: 'GET', url: '/api/task/list' });
      expect(listResponse.statusCode).to.not.equal(404);
      const completeResponse = await fastify.inject({ method: 'POST', url: '/api/task/complete' });
      expect(completeResponse.statusCode).to.not.equal(404);
      const cancelResponse = await fastify.inject({ method: 'POST', url: '/api/task/cancel' });
      expect(cancelResponse.statusCode).to.not.equal(404);
      const retryResponse = await fastify.inject({ method: 'POST', url: '/api/task/retry' });
      expect(retryResponse.statusCode).to.not.equal(404);
      const nextResponse = await fastify.inject({ method: 'POST', url: '/api/task/next' });
      expect(nextResponse.statusCode).to.not.equal(404);
      const logResponse = await fastify.inject({ method: 'POST', url: '/api/task/log' });
      expect(logResponse.statusCode).to.not.equal(404);
      const callbackResponse = await fastify.inject({ method: 'POST', url: '/api/task/callback' });
      expect(callbackResponse.statusCode).to.not.equal(404);
      const statisticsResponse = await fastify.inject({ method: 'GET', url: '/api/task/statistics' });
      expect(statisticsResponse.statusCode).to.not.equal(404);
      const statisticsSseResponse = await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      expect(statisticsSseResponse.statusCode).to.not.equal(404);
    });
  });

  describe('服务功能测试 - create', () => {
    it('should create task successfully', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        userId: 'user-1', input: { name: 'test' }, type: 'test-type',
        targetId: 'target-1', targetType: 'document', runnerType: 'manual'
      });
      expect(task).to.exist;
      expect(task.type).to.equal('test-type');
      expect(task.status).to.equal('pending');
      expect(task.targetId).to.equal('target-1');
    });

    it('should throw error when task type is not defined', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.create({ type: 'undefined-type', targetId: 'target-1', targetType: 'document' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('不支持的任务类型');
      }
    });

    it('should set delayed start time when delay is provided', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const delay = 60;
      const beforeCreate = Date.now();
      const task = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document', delay });
      expect(task.startTime.getTime()).to.be.greaterThan(beforeCreate + 1000 * delay - 100);
    });
  });

  describe('服务功能测试 - detail', () => {
    it('should return task by id', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task).to.exist;
      expect(task.id).to.equal(created.id);
    });

    it('should throw error when task not found', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.detail({ id: 'non-existent' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('任务不存在');
      }
    });

    it('should throw error when id is empty', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.detail({ id: '' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('任务ID不能为空');
      }
    });

    it('should throw error when id is null', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.detail({ id: null });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('任务ID不能为空');
      }
    });
  });

  describe('服务功能测试 - list', () => {
    it('should return paginated task list', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.create({ type: 'test-type', targetId: 'target-2', targetType: 'image' });
      const result = await fastify.task.services.list({ perPage: 10, currentPage: 1 });
      expect(result.pageData).to.exist;
      expect(result.totalCount).to.equal(2);
    });

    it('should filter tasks by type', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.models.task.create({ type: 'other-type', targetId: 'target-2', targetType: 'image', status: 'pending' });
      const result = await fastify.task.services.list({ perPage: 10, currentPage: 1, filter: { type: 'test-type' } });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter tasks by status', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      const task2 = await fastify.task.services.create({ type: 'test-type', targetId: 'target-2', targetType: 'image' });
      await task2.update({ status: 'success' });
      const result = await fastify.task.services.list({ perPage: 10, currentPage: 1, filter: { status: 'success' } });
      expect(result.totalCount).to.equal(1);
    });
  });

  describe('服务功能测试 - cancel', () => {
    it('should cancel task by id', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.cancel({ id: created.id });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('canceled');
    });

    it('should not cancel completed task', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'success' });
      await fastify.task.services.cancel({ id: created.id });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('服务功能测试 - complete', () => {
    it('should complete task with success status', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.complete({ id: created.id, status: 'success', output: { result: 'done' }, userId: 'user-1' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
      expect(task.progress).to.equal(100);
    });

    it('should complete task with failed status', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.complete({ id: created.id, status: 'failed', error: 'Something went wrong', userId: 'user-1' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('服务功能测试 - retry', () => {
    it('should retry failed task', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'failed' });
      await fastify.task.services.retry({ id: created.id });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('pending');
    });

    it('should throw error when retrying non-failed task', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      try {
        await fastify.task.services.retry({ id: created.id });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('只有失败或取消的任务允许重试');
      }
    });
  });

  describe('服务功能测试 - resetAll', () => {
    it('should reset all running tasks to pending', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task1 = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      const task2 = await fastify.task.services.create({ type: 'test-type', targetId: 'target-2', targetType: 'document' });
      await task1.update({ status: 'running' });
      await task2.update({ status: 'running' });
      await fastify.task.services.resetAll();
      const result = await fastify.task.services.list({ perPage: 10, currentPage: 1, filter: { status: 'pending' } });
      expect(result.totalCount).to.equal(2);
    });
  });

  describe('服务功能测试 - log', () => {
    it('should add log to task', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.log({ taskId: created.id, message: 'Test log message', data: { key: 'value' } });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log message');
    });
  });

  describe('边界情况测试', () => {
    it('should handle null input gracefully', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document', input: null });
      expect(task).to.exist;
      expect(task.input).to.be.null;
    });

    it('should handle empty input object', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document', input: {} });
      expect(task).to.exist;
      expect(task.input).to.deep.equal({});
    });
  });

  describe('签名验证测试 - processNext', () => {
    it('should pass with valid signature when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'waiting', context: { secret } });
      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });
      const dataToSign = `${created.id}|${resultStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');
      await fastify.task.services.processNext({ id: created.id, signature, result: resultStr });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });

    it('should fail with invalid signature when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'waiting', context: { secret } });
      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });
      try {
        await fastify.task.services.processNext({ id: created.id, signature: 'invalid-signature', result: resultStr });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('签名验证失败');
      }
    });

    it('should pass without signature when secret is not set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'waiting' });
      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });
      await fastify.task.services.processNext({ id: created.id, result: resultStr });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('签名验证测试 - logWithSignature', () => {
    it('should pass with valid signature when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      const data = { key: 'value' };
      const message = 'Test log';
      const dataStr = JSON.stringify({ data, message });
      const dataToSign = `${created.id}|${dataStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');
      await fastify.task.services.logWithSignature({ id: created.id, data, message, signature });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log');
    });

    it('should fail with invalid signature when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      try {
        await fastify.task.services.logWithSignature({ id: created.id, data: { key: 'value' }, message: 'Test log', signature: 'invalid-signature' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('签名验证失败');
      }
    });

    it('should pass without signature when secret is not set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.logWithSignature({ id: created.id, data: { key: 'value' }, message: 'Test log' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log');
    });
  });

  describe('签名验证测试 - callbackWithSignature', () => {
    it('should pass with valid signature when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      const code = 0;
      const data = { result: 'done' };
      const message = 'Success';
      const resultStr = JSON.stringify({ code, data, message });
      const dataToSign = `${created.id}|${resultStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');
      await fastify.task.services.callbackWithSignature({ id: created.id, code, data, message, signature });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });

    it('should fail with invalid signature when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      try {
        await fastify.task.services.callbackWithSignature({ id: created.id, code: 0, data: { result: 'done' }, message: 'Success', signature: 'invalid-signature' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('签名验证失败');
      }
    });

    it('should pass without signature when secret is not set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.callbackWithSignature({ id: created.id, code: 0, data: { result: 'done' }, message: 'Success' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('内部调用测试 - log/callback', () => {
    it('log should work without signature even when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      await fastify.task.services.log({ id: created.id, data: { key: 'value' }, message: 'Test log' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log');
    });

    it('callback should work without signature even when secret is set', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      await fastify.task.services.callback({ id: created.id, code: 0, data: { result: 'done' }, message: 'Success' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  // 补充覆盖率：log 内部异常路径
  describe('log 异常路径测试', () => {
    it('log should wrap non-TaskError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      // Mock task.update to throw a generic Error
      const task = await fastify.task.services.detail({ id: created.id });
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('DB connection lost'); };
      try {
        await fastify.task.services.log({ id: created.id, message: 'test' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
      task.update = originalUpdate;
    });

    it('logWithSignature should wrap non-TaskValidationError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      // Mock task.update to throw a generic Error (after signature verification passes)
      const task = await fastify.task.services.detail({ id: created.id });
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('DB down'); };
      const data = { key: 'value' };
      const message = 'Test log';
      const dataStr = JSON.stringify({ data, message });
      const dataToSign = `${created.id}|${dataStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');
      try {
        await fastify.task.services.logWithSignature({ id: created.id, data, message, signature });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
      task.update = originalUpdate;
    });
  });

  // 补充覆盖率：callback 内部异常路径
  describe('callback 异常路径测试', () => {
    it('callback should wrap non-TaskError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      // Make complete throw a generic error by corrupting task.update
      const task = await fastify.task.services.detail({ id: created.id });
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('DB error'); };
      try {
        await fastify.task.services.callback({ id: created.id, code: 1, data: {}, message: 'fail' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
      task.update = originalUpdate;
    });

    it('callbackWithSignature should wrap non-TaskValidationError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ context: { secret } });
      // Make the underlying callback fail by corrupting task.update
      const task = await fastify.task.services.detail({ id: created.id });
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('DB error'); };
      const dataStr = JSON.stringify({ code: 0, data: {}, message: 'Success' });
      const dataToSign = `${created.id}|${dataStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');
      try {
        await fastify.task.services.callbackWithSignature({ id: created.id, code: 0, data: {}, message: 'Success', signature });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
      task.update = originalUpdate;
    });
  });

  // 补充覆盖率：processNext TaskExecutionError 路径
  describe('processNext 异常路径测试', () => {
    it('processNext should wrap non-TaskStatus/ValidationError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'waiting' });
      // Make task.update throw a generic error
      const task = await fastify.task.services.detail({ id: created.id });
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('DB error'); };
      const resultStr = JSON.stringify({ code: 1, data: null, message: 'fail' });
      try {
        await fastify.task.services.processNext({ id: created.id, result: resultStr });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
      task.update = originalUpdate;
    });
  });

  // 补充覆盖率：processNext 验证路径
  describe('processNext 验证路径测试', () => {
    it('should throw TaskValidationError when result is not valid JSON object', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'waiting' });
      try {
        await fastify.task.services.processNext({ id: created.id, result: 'not-json-object' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_VALIDATION_ERROR');
      }
    });
  });

  // 补充覆盖率：create 异常路径
  describe('create 异常路径测试', () => {
    it('should throw TaskExecutionError when models.task.create fails', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      // Corrupt the model's create method
      const originalCreate = fastify.task.models.task.create;
      fastify.task.models.task.create = async () => { throw new Error('DB connection failed'); };
      try {
        await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
      fastify.task.models.task.create = originalCreate;
    });
  });

  // 补充覆盖率：log/callback TaskNotFoundError 直接抛出路径
  describe('log/callback TaskNotFoundError 直接抛出路径', () => {
    it('log should re-throw TaskNotFoundError directly', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.log({ id: 'nonexistent-id', message: 'test' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_NOT_FOUND');
      }
    });

    it('callback should re-throw TaskNotFoundError directly', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.callback({ id: 'nonexistent-id', code: 0, data: {}, message: '' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.code).to.equal('TASK_NOT_FOUND');
      }
    });

    it('callbackWithSignature should wrap TaskNotFoundError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.callbackWithSignature({ id: 'nonexistent-id', code: 0, data: {}, message: '', signature: 'sig' });
        throw new Error('Should have thrown');
      } catch (e) {
        // callbackWithSignature only re-throws TaskValidationError, others become TaskExecutionError
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
    });

    it('logWithSignature should wrap TaskNotFoundError as TaskExecutionError', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.logWithSignature({ id: 'nonexistent-id', data: {}, message: 'test', signature: 'sig' });
        throw new Error('Should have thrown');
      } catch (e) {
        // logWithSignature only re-throws TaskValidationError, others become TaskExecutionError
        expect(e.code).to.equal('TASK_EXECUTION_ERROR');
      }
    });
  });

  // errorHandler 集成测试
  describe('errorHandler 统一错误处理测试', () => {
    it('should call errorHandler with type=callback when complete with failed status', async () => {
      const errorHandlerCalls = [];
      fastify = await createFastify({
        errorHandler: async ({ task, error, type }) => {
          errorHandlerCalls.push({ task, error, type });
        }
      }, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.complete({ id: created.id, status: 'failed', error: 'Something went wrong', userId: 'user-1' });
      expect(errorHandlerCalls.length).to.equal(1);
      expect(errorHandlerCalls[0].type).to.equal('callback');
      expect(errorHandlerCalls[0].task.id).to.equal(created.id);
    });

    it('should call errorHandler with type=execution when success handler throws', async () => {
      const errorHandlerCalls = [];
      fastify = await createFastify({
        task: {
          'error-type': async () => { throw new Error('handler failed'); }
        },
        errorHandler: async ({ task, error, type }) => {
          errorHandlerCalls.push({ task, error, type });
        }
      }, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'error-type', targetId: 'target-1', targetType: 'document' });
      try {
        await fastify.task.services.complete({ id: created.id, status: 'success', output: { result: 'done' }, userId: 'user-1' });
      } catch (e) {
        // expected: handler throws
      }
      expect(errorHandlerCalls.length).to.equal(1);
      expect(errorHandlerCalls[0].type).to.equal('execution');
    });

    it('should call errorHandler with type=callback when processNext result code !== 0', async () => {
      const errorHandlerCalls = [];
      fastify = await createFastify({
        errorHandler: async ({ task, error, type }) => {
          errorHandlerCalls.push({ task, error, type });
        }
      }, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await created.update({ status: 'waiting' });
      const resultStr = JSON.stringify({ code: 1, data: null, message: 'callback failed' });
      await fastify.task.services.processNext({ id: created.id, result: resultStr });
      expect(errorHandlerCalls.length).to.equal(1);
      expect(errorHandlerCalls[0].type).to.equal('callback');
    });

    it('should not call errorHandler when task succeeds', async () => {
      const errorHandlerCalls = [];
      fastify = await createFastify({
        errorHandler: async ({ task, error, type }) => {
          errorHandlerCalls.push({ task, error, type });
        }
      }, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      await fastify.task.services.complete({ id: created.id, status: 'success', output: { result: 'done' }, userId: 'user-1' });
      expect(errorHandlerCalls.length).to.equal(0);
    });

    it('should not call errorHandler when errorHandler is null', async () => {
      fastify = await createFastify({ errorHandler: null }, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      // Should not throw even without errorHandler
      await fastify.task.services.complete({ id: created.id, status: 'failed', error: 'failed', userId: 'user-1' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });

    it('should not break main flow when errorHandler throws', async () => {
      fastify = await createFastify({
        errorHandler: async () => { throw new Error('errorHandler exploded'); }
      }, taskData, taskIdCounter);
      await fastify.ready();
      const created = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document' });
      // Main flow should still complete even if errorHandler throws
      await fastify.task.services.complete({ id: created.id, status: 'failed', error: 'Something went wrong', userId: 'user-1' });
      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });
});

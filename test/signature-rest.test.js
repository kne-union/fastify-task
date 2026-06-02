const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - signatures and rest endpoints', function () {
  this.timeout(10000);

  const context = createTestContext();
  let fastify;

  const createFastify = async options => {
    fastify = await context.createFastify(options);
    return fastify;
  };

  beforeEach(() => {
    context.reset();
  });

  afterEach(async () => {
    if (fastify) {
      await fastify.close();
      fastify = null;
    }
    context.restore();
  });

  describe('签名验证测试 - processNext', () => {
    it('should pass with valid signature when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting', context: { secret } });

      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });
      const dataToSign = `${created.id}|${resultStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');

      await fastify.task.services.processNext({
        id: created.id,
        signature,
        result: resultStr
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });

    it('should fail with invalid signature when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting', context: { secret } });

      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });

      try {
        await fastify.task.services.processNext({
          id: created.id,
          signature: 'invalid-signature',
          result: resultStr
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('签名验证失败');
      }
    });

    it('should pass without signature when secret is not set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting' });

      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });

      await fastify.task.services.processNext({
        id: created.id,
        result: resultStr
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('签名验证测试 - logWithSignature', () => {
    it('should pass with valid signature when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ context: { secret } });

      const data = { key: 'value' };
      const message = 'Test log';
      const dataStr = JSON.stringify({ data, message });
      const dataToSign = `${created.id}|${dataStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');

      await fastify.task.services.logWithSignature({
        id: created.id,
        data,
        message,
        signature
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log');
    });

    it('should fail with invalid signature when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ context: { secret } });

      try {
        await fastify.task.services.logWithSignature({
          id: created.id,
          data: { key: 'value' },
          message: 'Test log',
          signature: 'invalid-signature'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('签名验证失败');
      }
    });

    it('should pass without signature when secret is not set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.logWithSignature({
        id: created.id,
        data: { key: 'value' },
        message: 'Test log'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log');
    });
  });

  describe('签名验证测试 - callbackWithSignature', () => {
    it('should pass with valid signature when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ context: { secret } });

      const code = 0;
      const data = { result: 'done' };
      const message = 'Success';
      const resultStr = JSON.stringify({ code, data, message });
      const dataToSign = `${created.id}|${resultStr}`;
      const hmac = crypto.createHmac('sha256', secret);
      hmac.update(dataToSign);
      const signature = hmac.digest('hex');

      await fastify.task.services.callbackWithSignature({
        id: created.id,
        code,
        data,
        message,
        signature
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });

    it('should fail with invalid signature when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ context: { secret } });

      try {
        await fastify.task.services.callbackWithSignature({
          id: created.id,
          code: 0,
          data: { result: 'done' },
          message: 'Success',
          signature: 'invalid-signature'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('签名验证失败');
      }
    });

    it('should pass without signature when secret is not set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.callbackWithSignature({
        id: created.id,
        code: 0,
        data: { result: 'done' },
        message: 'Success'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('内部调用测试 - log/callback', () => {
    it('log should work without signature even when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ context: { secret } });

      await fastify.task.services.log({
        id: created.id,
        data: { key: 'value' },
        message: 'Test log'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log');
    });

    it('callback should work without signature even when secret is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const secret = 'test-secret-key';
      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ context: { secret } });

      await fastify.task.services.callback({
        id: created.id,
        code: 0,
        data: { result: 'done' },
        message: 'Success'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('REST 接口集成测试', () => {
    it('POST /create should create task and return id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/create',
        payload: {
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document'
        }
      });
      expect(response.statusCode).to.equal(200);
      const body = JSON.parse(response.payload);
      expect(body).to.have.property('id');
    });

    it('POST /complete should complete task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/complete',
        payload: {
          id: created.id,
          status: 'success',
          output: { result: 'done' }
        }
      });
      expect(response.statusCode).to.equal(200);
    });

    it('POST /retry should retry task via REST', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'failed' });

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/retry',
        payload: { id: created.id }
      });
      expect(response.statusCode).to.equal(200);

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('pending');
    });

    it('POST /next should process next', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting', context: {} });

      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });
      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/next',
        payload: { id: created.id, result: resultStr }
      });
      expect(response.statusCode).to.equal(200);
    });

    it('POST /log should record log with signature via REST', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/log',
        payload: { id: created.id, message: 'test log', data: { key: 'value' } }
      });
      expect(response.statusCode).to.equal(200);

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
    });

    it('POST /callback should process callback with signature via REST', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/callback',
        payload: { id: created.id, code: 0, data: { result: 'done' }, message: 'Success' }
      });
      expect(response.statusCode).to.equal(200);

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });

    it('POST /cancel should cancel task by id via REST', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/cancel',
        payload: { id: created.id }
      });
      expect(response.statusCode).to.equal(200);
    });
  });

  describe('processNext 错误路径测试', () => {
    it('should fail task when processNext receives non-zero code', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting', context: {} });

      const resultStr = JSON.stringify({ code: 1, message: 'External error' });
      await fastify.task.services.processNext({
        id: created.id,
        result: resultStr
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('processNext 非等待状态测试', () => {
    it('should throw error when processNext is called on non-waiting task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      try {
        await fastify.task.services.processNext({
          id: created.id,
          result: JSON.stringify({ code: 0, data: {} })
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('当前任务状态不允许执行Next操作');
      }
    });
  });

  describe('processNext 无 secret 验证测试', () => {
    it('should processNext without signature when context has no secret', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting', context: {} });

      const resultStr = JSON.stringify({ code: 0, data: { result: 'done' } });

      await fastify.task.services.processNext({
        id: created.id,
        result: resultStr
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('callback 非0 code 测试', () => {
    it('should complete with failed status when callback code is non-zero', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.callback({
        id: created.id,
        code: 1,
        data: null,
        message: 'Error occurred'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });
});

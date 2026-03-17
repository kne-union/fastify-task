const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');

describe('@kne/fastify-task', function () {
  this.timeout(10000);

  let fastify;
  let taskData = [];
  let taskIdCounter = 1;

  const mockUserModel = {
    findByPk: sinon.stub(),
    findOne: sinon.stub()
  };

  const mockAuthenticate = {
    user: async () => {},
    admin: async () => {},
    read: async () => {},
    write: async () => {}
  };

  // 创建模拟的 task 模型
  const createMockTaskModel = () => {
    return {
      create: async (data) => {
        const task = {
          id: `task-${taskIdCounter++}`,
          ...data,
          status: data.status || 'pending',
          progress: data.progress || 0,
          pollCount: data.pollCount || 0,
          pollResults: data.pollResults || [],
          context: data.context || {},
          options: data.options || {},
          createdAt: new Date(),
          updatedAt: new Date(),
          update: async function (updateData) {
            Object.assign(this, updateData);
            this.updatedAt = new Date();
            return this;
          },
          reload: async function () {
            return this;
          }
        };
        taskData.push(task);
        return task;
      },
      findByPk: async (id) => {
        return taskData.find(t => t.id === id) || null;
      },
      findAll: async ({ where, limit }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => {
            for (const key in where) {
              if (t[key] !== where[key]) return false;
            }
            return true;
          });
        }
        return results.slice(0, limit);
      },
      findAndCountAll: async ({ where, offset, limit, order }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => {
            for (const key in where) {
              if (t[key] !== where[key]) return false;
            }
            return true;
          });
        }
        return {
          rows: results.slice(offset, offset + limit),
          count: results.length
        };
      },
      count: async ({ where }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => {
            for (const key in where) {
              if (t[key] !== where[key]) return false;
            }
            return true;
          });
        }
        return results.length;
      },
      update: async (updateData, { where }) => {
        let count = 0;
        taskData.forEach(t => {
          let match = true;
          for (const key in where) {
            if (t[key] !== where[key]) match = false;
          }
          if (match) {
            Object.assign(t, updateData);
            count++;
          }
        });
        return [count];
      }
    };
  };

  const createFastify = async (options = {}) => {
    const app = require('fastify')();
    const Op = {
      in: Symbol('in'),
      lte: Symbol('lte'),
      gte: Symbol('gte'),
      between: Symbol('between'),
      like: Symbol('like')
    };

    const mockTaskModel = createMockTaskModel();

    // 模拟 sequelize
    app.decorate('sequelize', {
      Sequelize: { Op },
      models: { task: mockTaskModel }
    });

    // 模拟 account
    app.decorate('account', {
      models: { user: mockUserModel },
      authenticate: mockAuthenticate
    });

    // 模拟 cron
    app.decorate('cron', {
      createJob: sinon.stub()
    });

    // 创建 task 命名空间
    const taskOptions = {
      dbTableNamePrefix: 't_',
      prefix: '/api/task',
      name: 'task',
      limit: 10,
      dir: path.resolve(__dirname, './tasks'),
      cronTime: null,
      scriptName: 'index',
      maxPollTimes: 20,
      pollInterval: 100,
      task: {
        'test-type': async ({ task, result }) => {
          return result;
        }
      },
      getUserModel: () => mockUserModel,
      getAuthenticate: () => [mockAuthenticate.user, mockAuthenticate.admin],
      ...options
    };

    app.decorate('task', {
      options: taskOptions,
      models: { task: mockTaskModel },
      services: {},
      controllers: {}
    });

    // 加载 services
    const serviceModule = require('../libs/services/main');
    await serviceModule(app, taskOptions);

    // 加载 controllers
    const controllerModule = require('../libs/controllers/main');
    await controllerModule(app, taskOptions);

    return app;
  };

  beforeEach(() => {
    taskData = [];
    taskIdCounter = 1;
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
      fastify = await createFastify();
      await fastify.ready();

      expect(fastify.task).to.exist;
      expect(fastify.task.services).to.exist;
      expect(fastify.task.models).to.exist;
    });

    it('should register plugin with custom options', async () => {
      fastify = await createFastify({ name: 'task', limit: 5 });
      await fastify.ready();

      expect(fastify.task).to.exist;
      expect(fastify.task.options.limit).to.equal(5);
    });

    it('should expose all required services', async () => {
      fastify = await createFastify();
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
      fastify = await createFastify();
      await fastify.ready();

      // 通过 inject 测试路由是否可访问
      const listResponse = await fastify.inject({
        method: 'GET',
        url: '/api/task/list'
      });
      expect(listResponse.statusCode).to.not.equal(404);

      const completeResponse = await fastify.inject({
        method: 'POST',
        url: '/api/task/complete'
      });
      expect(completeResponse.statusCode).to.not.equal(404);

      const cancelResponse = await fastify.inject({
        method: 'POST',
        url: '/api/task/cancel'
      });
      expect(cancelResponse.statusCode).to.not.equal(404);

      const retryResponse = await fastify.inject({
        method: 'POST',
        url: '/api/task/retry'
      });
      expect(retryResponse.statusCode).to.not.equal(404);

      const nextResponse = await fastify.inject({
        method: 'POST',
        url: '/api/task/next'
      });
      expect(nextResponse.statusCode).to.not.equal(404);

      const logResponse = await fastify.inject({
        method: 'POST',
        url: '/api/task/log'
      });
      expect(logResponse.statusCode).to.not.equal(404);

      const callbackResponse = await fastify.inject({
        method: 'POST',
        url: '/api/task/callback'
      });
      expect(callbackResponse.statusCode).to.not.equal(404);
    });
  });

  describe('服务功能测试 - create', () => {
    it('should create task successfully', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        userId: 'user-1',
        input: { name: 'test' },
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      expect(task).to.exist;
      expect(task.type).to.equal('test-type');
      expect(task.status).to.equal('pending');
      expect(task.targetId).to.equal('target-1');
    });

    it('should throw error when task type is not defined', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'undefined-type',
          targetId: 'target-1',
          targetType: 'document'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('未找到合法的任务声明');
      }
    });

    it('should set delayed start time when delay is provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const delay = 60;
      const beforeCreate = Date.now();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        delay
      });

      expect(task.startTime.getTime()).to.be.greaterThan(beforeCreate + 1000 * delay - 100);
    });
  });

  describe('服务功能测试 - detail', () => {
    it('should return task by id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task).to.exist;
      expect(task.id).to.equal(created.id);
    });

    it('should throw error when task not found', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.detail({ id: 'non-existent' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('任务不存在');
      }
    });
  });

  describe('服务功能测试 - list', () => {
    it('should return paginated task list', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image'
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1
      });

      expect(result.pageData).to.exist;
      expect(result.totalCount).to.equal(2);
    });

    it('should filter tasks by type', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      // 直接创建一个不同类型的任务到数据库，绕过 create 的类型验证
      const otherTask = await fastify.task.models.task.create({
        type: 'other-type',
        targetId: 'target-2',
        targetType: 'image',
        status: 'pending'
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { type: 'test-type' }
      });

      expect(result.totalCount).to.equal(1);
    });

    it('should filter tasks by status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image'
      });
      await task2.update({ status: 'success' });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { status: 'success' }
      });

      expect(result.totalCount).to.equal(1);
    });
  });

  describe('服务功能测试 - cancel', () => {
    it('should cancel task by id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('canceled');
    });

    it('should not cancel completed task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'success' });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('服务功能测试 - complete', () => {
    it('should complete task with success status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
      expect(task.progress).to.equal(100);
    });

    it('should complete task with failed status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'failed',
        error: 'Something went wrong',
        userId: 'user-1'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('服务功能测试 - retry', () => {
    it('should retry failed task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'failed' });

      await fastify.task.services.retry({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('pending');
    });

    it('should throw error when retrying non-failed task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

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
      fastify = await createFastify();
      await fastify.ready();

      const task1 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document'
      });
      await task1.update({ status: 'running' });
      await task2.update({ status: 'running' });

      await fastify.task.services.resetAll();

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { status: 'pending' }
      });

      expect(result.totalCount).to.equal(2);
    });
  });

  describe('服务功能测试 - log', () => {
    it('should add log to task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.log({
        taskId: created.id,
        message: 'Test log message',
        data: { key: 'value' }
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log message');
    });
  });

  describe('边界情况测试', () => {
    it('should handle null input gracefully', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        input: null
      });

      expect(task).to.exist;
      expect(task.input).to.be.null;
    });

    it('should handle empty input object', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        input: {}
      });

      expect(task).to.exist;
      expect(task.input).to.deep.equal({});
    });
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
});

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

  const getValueByPath = (obj, path) => {
    return path.split('.').reduce((acc, key) => (acc == null ? acc : acc[key]), obj);
  };

  const matchOperatorCondition = (fieldValue, condition, Op) => {
    if (!condition || typeof condition !== 'object' || Array.isArray(condition)) {
      return fieldValue === condition;
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.between)) {
      const [start, end] = condition[Op.between] || [];
      return fieldValue >= start && fieldValue <= end;
    }
    const rangeChecks = [];
    if (Object.prototype.hasOwnProperty.call(condition, Op.gte)) rangeChecks.push(fieldValue >= condition[Op.gte]);
    if (Object.prototype.hasOwnProperty.call(condition, Op.gt)) rangeChecks.push(fieldValue > condition[Op.gt]);
    if (Object.prototype.hasOwnProperty.call(condition, Op.lte)) rangeChecks.push(fieldValue <= condition[Op.lte]);
    if (Object.prototype.hasOwnProperty.call(condition, Op.lt)) rangeChecks.push(fieldValue < condition[Op.lt]);
    if (rangeChecks.length) return rangeChecks.every(Boolean);
    if (Object.prototype.hasOwnProperty.call(condition, Op.in)) {
      const list = condition[Op.in] || [];
      return list.includes(fieldValue);
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.ne)) {
      return fieldValue !== condition[Op.ne];
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.like)) {
      const pattern = String(condition[Op.like] || '');
      const normalized = pattern.replaceAll('%', '');
      return String(fieldValue || '').includes(normalized);
    }
    return fieldValue === condition;
  };

  const matchWhere = (item, where = {}, Op) => {
    if (!where || typeof where !== 'object') return true;
    const orBranches = Op.or != null ? where[Op.or] : undefined;
    const rest = { ...where };
    if (Op.or != null && Object.prototype.hasOwnProperty.call(rest, Op.or)) {
      delete rest[Op.or];
    }

    const matchRest = Object.entries(rest).every(([key, condition]) => {
      const fieldValue = getValueByPath(item, key);
      return matchOperatorCondition(fieldValue, condition, Op);
    });
    if (orBranches == null) return matchRest;

    const branches = Array.isArray(orBranches) ? orBranches : [orBranches];
    const matchOr = branches.some(branch => {
      if (branch && typeof branch === 'object' && !Array.isArray(branch)) {
        return Object.entries(branch).every(([key, condition]) => {
          const fieldValue = getValueByPath(item, key);
          return matchOperatorCondition(fieldValue, condition, Op);
        });
      }
      return false;
    });

    return matchRest && matchOr;
  };

  const createMockTaskModel = Op => {
    return {
      create: async data => {
        const task = {
          id: `task-${taskIdCounter++}`,
          ...data,
          status: data.status || 'pending',
          progress: data.progress || 0,
          pollCount: data.pollCount || 0,
          pollResults: data.pollResults || [],
          context: data.context || {},
          options: data.options || {},
          priority: data.priority || 0,
          parentTaskId: data.parentTaskId || null,
          retryCount: data.retryCount || 0,
          maxRetries: data.maxRetries || 0,
          completedUserId: data.completedUserId || null,
          input: data.input !== undefined ? data.input : null,
          output: data.output !== undefined ? data.output : null,
          createdAt: data.createdAt || new Date(),
          updatedAt: data.updatedAt || new Date(),
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
      findByPk: async id => {
        return taskData.find(t => t.id === id) || null;
      },
      findAll: async ({ where, limit, attributes, group, order } = {}) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
        }

        if (order && Array.isArray(order)) {
          results = [...results].sort((a, b) => {
            for (const [key, direction] of order) {
              const aVal = a[key];
              const bVal = b[key];
              if (aVal < bVal) return direction === 'DESC' ? 1 : -1;
              if (aVal > bVal) return direction === 'DESC' ? -1 : 1;
            }
            return 0;
          });
        }

        return results.slice(0, limit);
      },
      findAndCountAll: async ({ where, offset, limit, order }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
        }

        if (order && Array.isArray(order)) {
          results = [...results].sort((a, b) => {
            for (const [key, direction] of order) {
              const aVal = a[key];
              const bVal = b[key];
              if (aVal < bVal) return direction === 'DESC' ? 1 : -1;
              if (aVal > bVal) return direction === 'DESC' ? -1 : 1;
            }
            return 0;
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
          results = taskData.filter(t => matchWhere(t, where, Op));
        }
        return results.length;
      },
      update: async (updateData, { where }) => {
        let count = 0;
        taskData.forEach(t => {
          if (matchWhere(t, where, Op)) {
            Object.assign(t, updateData);
            count++;
          }
        });
        return [count];
      },
      rawAttributes: {
        createdAt: { field: 'created_at' },
        completedAt: { field: 'completed_at' }
      },
      sequelize: {
        Sequelize: {
          Op,
          fn: sinon.stub().callsFake((fn, ...args) => ({ fn, args })),
          col: sinon.stub().callsFake(col => ({ col })),
          literal: sinon.stub().callsFake(lit => ({ literal: lit }))
        },
        getDialect: sinon.stub().returns('sqlite')
      }
    };
  };

  const createFastify = async (options = {}) => {
    const app = require('fastify')();
    const Op = {
      in: Symbol('in'),
      lte: Symbol('lte'),
      gte: Symbol('gte'),
      lt: Symbol('lt'),
      gt: Symbol('gt'),
      between: Symbol('between'),
      ne: Symbol('ne'),
      like: Symbol('like'),
      or: Symbol('or')
    };

    const mockTaskModel = createMockTaskModel(Op);

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

    // 模拟 @kne/fastify-statistics
    app.decorate('taskStatistics', {
      services: {
        collect: sinon.stub().resolves(),
        query: sinon.stub().resolves({ channelMetas: {}, list: [] }),
        sseStream: {
          send: sinon.stub().resolves()
        }
      }
    });

    // 模拟 log（Fastify内置log，不能重新decorate，用sinon替换方法）
    const originalLog = app.log;
    const mockLog = {
      info: sinon.stub(),
      error: sinon.stub(),
      warn: sinon.stub()
    };
    app.log = mockLog;

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
        },
        'polling-type': async ({ task, result }) => {
          return result;
        },
        'next-type': async ({ task, result, context }) => {
          return result;
        },
        'progress-type': async ({ task, result }) => {
          return result;
        },
        'fail-type': async ({ task, result }) => {
          return result;
        },
        'hang-type': async ({ task, result }) => {
          return result;
        },
        'log-type': async ({ task, result }) => {
          return result;
        },
        'polling-fail-type': async ({ task, result }) => {
          return result;
        },
        'polling-pending-type': async ({ task, result }) => {
          return result;
        }
      },
      getUserModel: () => mockUserModel,
      getAuthenticate: () => [mockAuthenticate.user, mockAuthenticate.admin],
      ...options
    };

    // 初始化 dirs：与 index.js 保持一致
    if (!taskOptions.dirs) {
      taskOptions.dirs = [taskOptions.dir];
    } else if (!taskOptions.dirs.includes(taskOptions.dir)) {
      taskOptions.dirs = [taskOptions.dir, ...taskOptions.dirs];
    }

    app.decorate('task', {
      options: taskOptions,
      models: { task: mockTaskModel },
      services: {},
      controllers: {}
    });

    // 加载 services
    const serviceModule = require('../libs/services/main');
    await serviceModule(app, taskOptions);

    // 加载 statistics service
    const statisticsServiceModule = require('../libs/services/statistics');
    await statisticsServiceModule(app, taskOptions);

    // 加载 controllers
    const controllerModule = require('../libs/controllers/main');
    await controllerModule(app, taskOptions);

    // 加载 statistics controller
    const statisticsModule = require('../libs/controllers/statistics');
    await statisticsModule(app, taskOptions);

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

      const statisticsResponse = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics'
      });
      expect(statisticsResponse.statusCode).to.not.equal(404);

      const statisticsSseResponse = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse'
      });
      expect(statisticsSseResponse.statusCode).to.not.equal(404);
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

  describe('startedAt 字段测试', () => {
    it('should set startedAt when complete is called', async () => {
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
      expect(task.startedAt).to.exist;
      expect(task.startedAt).to.be.instanceOf(Date);
    });

    it('should set startedAt when complete with failed status', async () => {
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
      expect(task.startedAt).to.exist;
    });

    it('should set completedAt when cancel is called', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.completedAt).to.exist;
      expect(task.status).to.equal('canceled');
    });
  });

  describe('collectTaskStatistics 测试', () => {
    it('should call fastify.statistics.services.collect when task completes', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(fastify.taskStatistics.services.collect.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.collect.firstCall.args[0];
      expect(callArgs.channel).to.equal('task:test-type:manual');
      expect(callArgs.data.total).to.equal(1);
      expect(callArgs.data.success).to.equal(1);
      expect(callArgs.time).to.exist;
    });

    it('should call collect with failed status when task fails', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'failed',
        error: 'Something went wrong',
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(fastify.taskStatistics.services.collect.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.collect.firstCall.args[0];
      expect(callArgs.channel).to.equal('task:test-type:system');
      expect(callArgs.data.failed).to.equal(1);
    });

    it('should call collect with canceled status when task is canceled', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      await fastify.task.services.cancel({ id: created.id });

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(fastify.taskStatistics.services.collect.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.collect.firstCall.args[0];
      expect(callArgs.data.canceled).to.equal(1);
    });

    it('should not throw when collect fails', async () => {
      fastify = await createFastify();
      await fastify.ready();

      fastify.taskStatistics.services.collect = sinon.stub().rejects(new Error('collect error'));

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      // 不应该抛出异常
      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      // 应该记录了错误日志
      expect(fastify.log.error.called).to.be.true;
    });
  });

  describe('统计接口路由测试', () => {
    it('should register statistics route', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d' }
      });
      expect(response.statusCode).to.equal(200);
      const body = JSON.parse(response.payload);
      expect(body).to.have.property('channelMetas');
      expect(body).to.have.property('list');
    });

    it('should call statistics query with correct channel when type is specified', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d', type: 'test-type' }
      });

      expect(fastify.taskStatistics.services.query.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.channels).to.deep.equal(['task:test-type']);
      expect(callArgs.includeChildren).to.be.false;
    });

    it('should call statistics query with task channel and includeChildren when no filter', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d' }
      });

      expect(fastify.taskStatistics.services.query.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.channels).to.deep.equal(['task']);
      expect(callArgs.includeChildren).to.be.true;
    });

    it('should call statistics query with type and runnerType channel', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d', type: 'test-type', runnerType: 'manual' }
      });

      expect(fastify.taskStatistics.services.query.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.channels).to.deep.equal(['task:test-type:manual']);
      expect(callArgs.includeChildren).to.be.false;
    });

    it('should throw error for unsupported range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '1w' }
      });
      expect(response.statusCode).to.equal(500);
    });
  });

  describe('append 接口测试', () => {
    it('should append new dirs', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.append({
        dirs: ['/path/to/dir1', '/path/to/dir2']
      });

      expect(result.dirs).to.deep.equal(['/path/to/dir1', '/path/to/dir2']);
      expect(fastify.task.options.dirs).to.include('/path/to/dir1');
      expect(fastify.task.options.dirs).to.include('/path/to/dir2');
    });

    it('should skip existing dirs when appending', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const existingDir = fastify.task.options.dirs[0];
      const result = await fastify.task.services.append({
        dirs: [existingDir, '/path/to/new-dir']
      });

      expect(result.dirs).to.deep.equal(['/path/to/new-dir']);
    });

    it('should append new tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const handler = async ({ task, result }) => result;
      const result = await fastify.task.services.append({
        tasks: {
          'new-task-type': handler
        }
      });

      expect(result.tasks).to.deep.equal(['new-task-type']);
      expect(fastify.task.options.task['new-task-type']).to.equal(handler);
    });

    it('should skip existing tasks when appending', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const handler = async ({ task, result }) => result;
      const result = await fastify.task.services.append({
        tasks: {
          'test-type': handler
        }
      });

      expect(result.tasks).to.deep.equal([]);
    });

    it('should throw error when task handler is not a function', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.append({
          tasks: {
            'bad-task': 'not-a-function'
          }
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('handler 必须是一个函数');
      }
    });

    it('should append both dirs and tasks at the same time', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const handler = async ({ task, result }) => result;
      const result = await fastify.task.services.append({
        dirs: ['/path/to/dir3'],
        tasks: {
          'combined-task': handler
        }
      });

      expect(result.dirs).to.deep.equal(['/path/to/dir3']);
      expect(result.tasks).to.deep.equal(['combined-task']);
    });

    it('should allow creating tasks with newly appended task type', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const handler = async ({ task, result }) => result;
      await fastify.task.services.append({
        tasks: {
          'dynamic-task': handler
        }
      });

      const task = await fastify.task.services.create({
        type: 'dynamic-task',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task).to.exist;
      expect(task.type).to.equal('dynamic-task');
    });
  });

  describe('dirs 选项兼容性测试', () => {
    it('should use dir as default when dirs is not provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      expect(fastify.task.options.dirs).to.be.an('array');
      expect(fastify.task.options.dirs[0]).to.equal(fastify.task.options.dir);
    });

    it('should merge dir into dirs when dirs is provided without dir', async () => {
      const customDir = '/custom/tasks';
      fastify = await createFastify({ dirs: [customDir] });
      await fastify.ready();

      expect(fastify.task.options.dirs).to.include(fastify.task.options.dir);
      expect(fastify.task.options.dirs).to.include(customDir);
    });
  });

  describe('升级功能测试 - 任务优先级 (#1)', () => {
    it('should create task with priority', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        priority: 10
      });

      expect(task.priority).to.equal(10);
    });

    it('should default priority to 0', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task.priority).to.equal(0);
    });

    it('should throw error for non-integer priority', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          priority: 1.5
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('priority 必须为整数');
      }
    });
  });

  describe('升级功能测试 - 任务依赖 (#2)', () => {
    it('should create task with parentTaskId', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const parent = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const child = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        parentTaskId: parent.id
      });

      expect(child.parentTaskId).to.equal(parent.id);
    });
  });

  describe('升级功能测试 - 重试策略 (#4)', () => {
    it('should create task with maxRetries', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        maxRetries: 3
      });

      expect(task.maxRetries).to.equal(3);
      expect(task.retryCount).to.equal(0);
    });

    it('should throw error for negative maxRetries', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          maxRetries: -1
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('maxRetries 必须为非负整数');
      }
    });

    it('should reset retryCount when retry is called', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'failed', retryCount: 2 });

      await fastify.task.services.retry({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.retryCount).to.equal(0);
      expect(task.error).to.be.null;
    });
  });

  describe('升级功能测试 - completedUserId 字段 (#6)', () => {
    it('should have completedUserId field in task model', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task).to.have.property('completedUserId');
    });
  });

  describe('升级功能测试 - list 参数默认值 (#9)', () => {
    it('should use default perPage and currentPage when not provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const result = await fastify.task.services.list({});

      expect(result.pageData).to.exist;
      expect(result.totalCount).to.equal(1);
    });
  });

  describe('升级功能测试 - cancel 校验位置 (#10)', () => {
    it('should throw error when neither id nor targetId+targetType+type is provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.cancel({});
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('必须提供 id 或 targetId+targetType+type');
      }
    });
  });

  describe('升级功能测试 - create REST 接口 (#14)', () => {
    it('should register create route', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/create'
      });
      expect(response.statusCode).to.not.equal(404);
    });
  });

  describe('升级功能测试 - cancel schema 补充 (#15)', () => {
    it('should accept targetId+targetType+type in cancel body', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'POST',
        url: '/api/task/cancel',
        payload: {
          targetId: 'target-1',
          targetType: 'document',
          type: 'test-type'
        }
      });
      // 不应 400（schema 校验失败）
      expect(response.statusCode).to.not.equal(400);
    });
  });

  describe('升级功能测试 - input/output 默认值 (#7)', () => {
    it('should have null as default for input and output', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task.input).to.be.null;
      expect(task.output).to.be.null;
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

  describe('list 过滤与排序测试', () => {
    it('should filter by targetName', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        input: { name: 'My Document' }
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image',
        input: { name: 'My Image' }
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { targetName: 'Document' }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by createdAt range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const now = new Date().toISOString();
      const past = new Date(Date.now() - 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { createdAt: { startTime: past, endTime: now } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by createdAt startTime only', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const past = new Date(Date.now() - 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { createdAt: { startTime: past } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by createdAt endTime only', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const future = new Date(Date.now() + 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { createdAt: { endTime: future } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by completedAt range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'success', completedAt: new Date() });

      const now = new Date().toISOString();
      const past = new Date(Date.now() - 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { completedAt: { startTime: past, endTime: now } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should sort by custom field', async () => {
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
        currentPage: 1,
        sort: { targetId: 'ASC' }
      });
      expect(result.totalCount).to.equal(2);
    });
  });

  describe('retry 批量测试', () => {
    it('should retry multiple tasks by taskIds', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task1 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await task1.update({ status: 'failed' });

      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image'
      });
      await task2.update({ status: 'canceled' });

      await fastify.task.services.retry({ taskIds: [task1.id, task2.id] });

      const t1 = await fastify.task.services.detail({ id: task1.id });
      expect(t1.status).to.equal('pending');

      const t2 = await fastify.task.services.detail({ id: task2.id });
      expect(t2.status).to.equal('pending');
    });
  });

  describe('log 超长截断测试', () => {
    it('should truncate logs when exceeding 100 entries', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      // 添加101条日志
      for (let i = 0; i < 101; i++) {
        await fastify.task.services.log({
          taskId: created.id,
          message: `Log ${i}`
        });
      }

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs.length).to.equal(100);
    });
  });

  describe('queryStatistics 时间范围分支测试', () => {
    it('should handle 1m range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '1m' });
      expect(result).to.exist;

      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.startTime).to.exist;
      expect(callArgs.endTime).to.exist;
    });

    it('should handle 3m range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '3m' });
      expect(result).to.exist;
    });

    it('should handle 1y range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '1y' });
      expect(result).to.exist;
    });

    it('should handle 7d range explicitly', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
    });

    it('should throw error for invalid range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.queryStatistics({ range: '1w' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('不支持的时间范围');
      }
    });

    it('should pass timezone to query', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.queryStatistics({ range: '7d', timezone: 'Asia/Shanghai' });

      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.timezone).to.equal('Asia/Shanghai');
    });
  });

  describe('SSE 统计接口测试', () => {
    it('should call sseStatistics with correct params', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse',
        query: { range: '7d', type: 'test-type', runnerType: 'manual' }
      });

      expect(fastify.taskStatistics.services.sseStream.send.calledOnce).to.be.true;
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      expect(sendArgs[1].name).to.equal('query');
      expect(sendArgs[1].params.channels).to.equal('task:test-type:manual');
      expect(sendArgs[1].params.includeChildren).to.be.false;
    });

    it('should call sseStatistics with includeChildren when no filter', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse',
        query: { range: '7d' }
      });

      expect(fastify.taskStatistics.services.sseStream.send.calledOnce).to.be.true;
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      expect(sendArgs[1].params.channels).to.equal('task');
      expect(sendArgs[1].params.includeChildren).to.be.true;
    });

    it('should call sseStatistics fetchData and return query result', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse',
        query: { range: '1m' }
      });

      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();
      expect(result).to.exist;
      // fetchData 内部调用了 query
      expect(fastify.taskStatistics.services.query.called).to.be.true;
    });
  });

  describe('collectTaskStatistics 时序数据测试', () => {
    it('should include timing data when task has startedAt', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });
      // 确保 createdAt 和 startedAt 有足够的时间差
      await created.update({ createdAt: new Date(Date.now() - 10000), startedAt: new Date(Date.now() - 5000) });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(fastify.taskStatistics.services.collect.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.collect.firstCall.args[0];
      expect(callArgs.data.waitingTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.data.executionTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.data.totalTime).to.be.a('number').and.greaterThan(0);
    });

    it('should calculate executionTime from totalTime when no startedAt', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 确保 createdAt 有足够的时间差，且没有 startedAt
      await created.update({ createdAt: new Date(Date.now() - 10000), startedAt: null });

      await fastify.task.services.complete({
        id: created.id,
        status: 'failed',
        error: 'test error',
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(fastify.taskStatistics.services.collect.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.collect.firstCall.args[0];
      expect(callArgs.data.executionTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.data.totalTime).to.be.a('number').and.greaterThan(0);
    });
  });

  describe('executor 系统任务执行器测试', () => {
    it('should execute system task via executor', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.progress).to.equal(100);
    });

    it('should update progress during task execution', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'progress-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should handle task executor error with retry', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 2
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      // 第一次失败，retryCount(1) < maxRetries(2)，应该重试
      expect(updated.status).to.equal('pending');
      expect(updated.retryCount).to.equal(1);
    });

    it('should fail task when max retries exceeded', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
    });

    it('should handle task timeout', async () => {
      fastify = await createFastify({ taskTimeout: 50 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'hang-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('超时');
    });

    it('should throw error for unmatched task executor', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      // 清空 dirs 模拟找不到执行器
      const originalDirs = fastify.task.options.dirs.slice();
      fastify.task.options.dirs = ['/nonexistent/path'];

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('未匹配到任务执行器');

      // 恢复
      fastify.task.options.dirs = originalDirs;
    });
  });

  describe('runner 调度测试', () => {
    it('should run pending system tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.runner();

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should skip runner when limit reached', async () => {
      fastify = await createFastify({ limit: 1 });
      await fastify.ready();

      // 创建一个正在运行的任务
      const runningTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await runningTask.update({ status: 'running' });

      // runner 应该跳过，因为 running 数量已达上限
      await fastify.task.services.runner();

      // 日志中应该记录跳过信息
      expect(fastify.log.info.called).to.be.true;
    });

    it('should execute task with higher priority first', async () => {
      fastify = await createFastify({ limit: 10 });
      await fastify.ready();

      const lowTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        priority: 1
      });

      const highTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'system',
        priority: 10
      });

      await fastify.task.services.runner();

      const highUpdated = await fastify.task.services.detail({ id: highTask.id });
      expect(highUpdated.status).to.equal('success');

      const lowUpdated = await fastify.task.services.detail({ id: lowTask.id });
      expect(lowUpdated.status).to.equal('success');
    });
  });

  describe('triggerChildTasks 子任务触发测试', () => {
    it('should trigger child system tasks after parent succeeds', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const parent = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      const child = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'system',
        parentTaskId: parent.id
      });

      // 执行父任务，应该触发子任务
      await fastify.task.services.processSystemTask(parent);

      const parentUpdated = await fastify.task.services.detail({ id: parent.id });
      expect(parentUpdated.status).to.equal('success');

      const childUpdated = await fastify.task.services.detail({ id: child.id });
      expect(childUpdated.status).to.equal('success');
    });

    it('should not trigger manual child tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const parent = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      const child = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'manual',
        parentTaskId: parent.id
      });

      await fastify.task.services.processSystemTask(parent);

      const childUpdated = await fastify.task.services.detail({ id: child.id });
      // manual 类型子任务保持 pending
      expect(childUpdated.status).to.equal('pending');
    });
  });

  describe('waitingComplete 测试', () => {
    it('should resolve when task completes via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      // waitingComplete 会自动执行 pending 的系统任务
      const result = await fastify.task.services.waitingComplete({
        id: task.id,
        pollInterval: 10,
        maxPollTimes: 50
      });

      expect(result).to.deep.equal({ result: 'success' });
    });

    it('should reject when task fails via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      try {
        await fastify.task.services.waitingComplete({
          id: task.id,
          pollInterval: 10,
          maxPollTimes: 50
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Task execution failed');
      }
    });

    it('should reject when task is canceled via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 设置为 running，避免 waitingComplete 自动执行
      await task.update({ status: 'running' });

      const completePromise = fastify.task.services.waitingComplete({
        id: task.id,
        pollInterval: 10,
        maxPollTimes: 50
      });

      setTimeout(async () => {
        await task.update({ status: 'canceled' });
      }, 50);

      try {
        await completePromise;
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('取消');
      }
    });

    it('should reject on timeout via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 设置为 running，避免 waitingComplete 自动执行
      await task.update({ status: 'running' });

      try {
        await fastify.task.services.waitingComplete({
          id: task.id,
          pollInterval: 10,
          maxPollTimes: 2
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('任务超时');
      }
    });
  });

  describe('cancel 批量操作测试', () => {
    it('should batch cancel by targetId+targetType+type', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      const affectedCount = await fastify.task.services.cancel({
        targetId: 'target-1',
        targetType: 'document',
        type: 'test-type'
      });

      expect(affectedCount).to.equal(2);
    });
  });

  describe('complete 异常路径测试', () => {
    it('should fail task when task handler throws error', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      // 让 task handler 抛出异常
      fastify.task.options.task['test-type'] = async () => {
        throw new Error('Handler error');
      };

      try {
        await fastify.task.services.complete({
          id: created.id,
          status: 'success',
          output: { result: 'done' },
          userId: 'user-1'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('Handler error');
      }

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
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

  describe('create 校验测试', () => {
    it('should throw error for negative delay', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          delay: -1
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('delay 必须为非负数');
      }
    });

    it('should throw error for non-numeric delay', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          delay: 'abc'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('delay 必须为非负数');
      }
    });

    it('should throw error for non-numeric priority', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          priority: 'high'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('priority 必须为整数');
      }
    });

    it('should throw error for non-integer maxRetries', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          maxRetries: 1.5
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('maxRetries 必须为非负整数');
      }
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

  describe('next-type 任务执行测试', () => {
    it('should set task to waiting status when executor calls next', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'next-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('waiting');
      expect(updated.context).to.deep.equal({ secret: 'test-secret' });
    });
  });

  describe('log-type 任务执行测试', () => {
    it('should record log when task executor calls log', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'log-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.options.logs).to.exist;
      expect(updated.options.logs[0].message).to.equal('Task log entry');
    });
  });

  describe('retry 指数退避测试', () => {
    it('should retry with exponential backoff delay', async () => {
      fastify = await createFastify({ retryBaseDelay: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 3
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('pending');
      expect(updated.retryCount).to.equal(1);
      // startTime 应该在未来（退避延迟）
      expect(updated.startTime.getTime()).to.be.greaterThan(Date.now() - 1000);
    });
  });

  describe('cancel running 任务测试', () => {
    it('should cancel running task by id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'running' });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('canceled');
    });
  });

  describe('retry canceled 任务测试', () => {
    it('should retry canceled task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'canceled' });

      await fastify.task.services.retry({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('pending');
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

  describe('cancel 批量无匹配测试', () => {
    it('should return 0 when no tasks match bulk cancel criteria', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const affectedCount = await fastify.task.services.cancel({
        targetId: 'nonexistent',
        targetType: 'document',
        type: 'test-type'
      });

      expect(affectedCount).to.equal(0);
    });
  });

  describe('append 警告测试', () => {
    it('should warn when appending non-existent dir', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.append({
        dirs: ['/nonexistent/path']
      });

      expect(result.dirs).to.deep.equal(['/nonexistent/path']);
      expect(fastify.task.options.dirs).to.include('/nonexistent/path');
    });
  });

  describe('统计接口默认参数测试', () => {
    it('should use default range when not provided in statistics query', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics'
      });
      expect(response.statusCode).to.equal(200);
    });

    it('should use default range and interval in SSE statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse'
      });

      expect(fastify.taskStatistics.services.sseStream.send.calledOnce).to.be.true;
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      expect(sendArgs[1].interval).to.equal(5);
    });
  });

  describe('taskTimeout=0 无超时测试', () => {
    it('should not apply timeout when taskTimeout is 0', async () => {
      fastify = await createFastify({ taskTimeout: 0 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });
  });

  describe('executor 自定义 scriptName 测试', () => {
    it('should use custom scriptName when provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        scriptName: 'index'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });
  });

  describe('list 自定义排序测试', () => {
    it('should list tasks with custom sort', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        priority: 5
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        priority: 10
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        sort: { priority: 'DESC' }
      });

      expect(result.totalCount).to.equal(2);
      expect(result.pageData[0].priority).to.equal(10);
      expect(result.pageData[1].priority).to.equal(5);
    });
  });

  describe('runner 无待处理任务测试', () => {
    it('should not execute when no pending tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.runner();

      // 不应抛出异常，正常运行
      expect(fastify.log.info.called).to.be.false;
    });
  });

  describe('collectTaskStatistics 无时间数据测试', () => {
    it('should not include timing data when task has no createdAt or completedAt', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });
      // 不设置 createdAt 和 completedAt 的时间差
      await created.update({ startedAt: null });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 50));

      expect(fastify.taskStatistics.services.collect.calledOnce).to.be.true;
      const callArgs = fastify.taskStatistics.services.collect.firstCall.args[0];
      expect(callArgs.data.total).to.equal(1);
      expect(callArgs.data.success).to.equal(1);
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

  describe('polling 轮询功能测试', () => {
    it('should execute polling-type task via processSystemTask', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.pollCount).to.be.greaterThan(0);
    });

    it('should handle polling with progress update', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should handle polling with custom options', async () => {
      fastify = await createFastify({ pollInterval: 10, maxPollTimes: 5 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should handle polling with failed result', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('任务处理失败');
    });

    it('should handle polling with pending then success result', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-pending-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.pollCount).to.be.greaterThan(0);
    });

    it('should handle polling timeout', async () => {
      fastify = await createFastify({ pollInterval: 10, maxPollTimes: 1 });
      await fastify.ready();

      // 添加一个永远返回 pending 的任务类型
      const pendingPollingHandler = async (fastify, options, { polling }) => {
        return await polling(async () => {
          return { result: 'pending' };
        });
      };

      // 直接使用 append 添加任务
      await fastify.task.services.append({
        tasks: {
          'always-pending-type': async ({ task, result }) => result
        }
      });

      // 创建任务脚本目录
      const fs = require('fs-extra');
      const tempDir = path.resolve(__dirname, './tasks/always-pending-type');
      await fs.ensureDir(tempDir);
      await fs.writeFile(path.resolve(tempDir, 'index.js'),
        `module.exports = async (fastify, options, { polling }) => {\n  return await polling(async () => {\n    return { result: 'pending' };\n  });\n};\n`
      );

      const task = await fastify.task.services.create({
        type: 'always-pending-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('轮询超时');

      // 清理临时文件
      await fs.remove(tempDir);
    });

    it('should handle polling callback throwing error', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      // 添加一个在 polling 中抛出异常的任务类型
      await fastify.task.services.append({
        tasks: {
          'polling-error-type': async ({ task, result }) => result
        }
      });

      const fs = require('fs-extra');
      const tempDir = path.resolve(__dirname, './tasks/polling-error-type');
      await fs.ensureDir(tempDir);
      await fs.writeFile(path.resolve(tempDir, 'index.js'),
        `module.exports = async (fastify, options, { polling }) => {\n  return await polling(async () => {\n    throw new Error('Polling callback error');\n  });\n};\n`
      );

      const task = await fastify.task.services.create({
        type: 'polling-error-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('Polling callback error');

      // 清理临时文件
      await fs.remove(tempDir);
    });
  });

  describe('waitingComplete 异常捕获测试', () => {
    it('should reject when reload throws error', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 设置为 running，避免 waitingComplete 自动执行
      await task.update({ status: 'running' });

      // 覆盖 reload 使其抛出异常
      const originalReload = task.reload;
      task.reload = async () => {
        throw new Error('Reload failed');
      };

      try {
        await fastify.task.services.waitingComplete({
          id: task.id,
          pollInterval: 10,
          maxPollTimes: 10
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('Reload failed');
      }

      // 恢复
      task.reload = originalReload;
    });
  });

  describe('complete 成功路径中 task handler 接收 result 测试', () => {
    it('should pass output as result to task handler on success', async () => {
      fastify = await createFastify();
      await fastify.ready();

      let handlerResult = null;
      fastify.task.options.task['test-type'] = async ({ task, result }) => {
        handlerResult = result;
        return result;
      };

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { data: 'test-output' },
        userId: 'user-1'
      });

      expect(handlerResult).to.deep.equal({ data: 'test-output' });
    });
  });

  describe('executor 未匹配执行器测试', () => {
    it('should throw error when no executor file found', async () => {
      fastify = await createFastify();
      await fastify.ready();

      // 添加一个只在 task 选项中声明但没有脚本文件的类型
      fastify.task.options.task['no-script-type'] = async ({ result }) => result;

      const task = await fastify.task.services.create({
        type: 'no-script-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('未匹配到任务执行器');
    });
  });
});

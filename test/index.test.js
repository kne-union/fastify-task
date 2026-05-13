const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');

dayjs.extend(utc);
dayjs.extend(timezone);

describe('@kne/fastify-task', function () {
  this.timeout(10000);

  let fastify;
  let taskData = [];
  let dailyStatsData = [];
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
    if (Object.prototype.hasOwnProperty.call(condition, Op.gte)) {
      return fieldValue >= condition[Op.gte];
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.lte)) {
      return fieldValue <= condition[Op.lte];
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.between)) {
      const [start, end] = condition[Op.between] || [];
      return fieldValue >= start && fieldValue <= end;
    }
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
    return Object.entries(where).every(([key, condition]) => {
      const fieldValue = getValueByPath(item, key);
      return matchOperatorCondition(fieldValue, condition, Op);
    });
  };

  const formatDate = date => {
    const d = new Date(date);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${m}-${day}`;
  };

  const formatHour = date => String(new Date(date).getHours()).padStart(2, '0');

  const formatQuarter = date => {
    const d = new Date(date);
    const h = String(d.getHours()).padStart(2, '0');
    const quarter = Math.floor(d.getMinutes() / 15) * 15;
    return `${h}:${String(quarter).padStart(2, '0')}`;
  };

  const resolveExprValue = (task, expr) => {
    if (typeof expr === 'string') return getValueByPath(task, expr);
    if (expr && typeof expr === 'object' && expr.col) return getValueByPath(task, expr.col);
    if (expr && typeof expr === 'object' && expr.fn === 'DATE') return formatDate(task.createdAt);
    if (expr && typeof expr === 'object' && expr.fn === 'strftime' && expr.args && expr.args[0] === '%H') {
      return formatHour(task.createdAt);
    }
    if (expr && typeof expr === 'object' && expr.literal && String(expr.literal).includes("strftime('%H'")) {
      return formatQuarter(task.createdAt);
    }
    return undefined;
  };

  // 创建模拟的 task 模型
  const createMockTaskModel = Op => {
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
      findByPk: async (id) => {
        return taskData.find(t => t.id === id) || null;
      },
      findAll: async ({ where, limit, attributes, group } = {}) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
        }

        if (group && attributes) {
          const groupItems = Array.isArray(group) ? group : [group];
          const grouped = new Map();

          results.forEach(task => {
            const groupValues = groupItems.map(item => resolveExprValue(task, item));
            const mapKey = JSON.stringify(groupValues);
            if (!grouped.has(mapKey)) {
              grouped.set(mapKey, { groupValues, count: 0 });
            }
            grouped.get(mapKey).count += 1;
          });

          const rows = Array.from(grouped.values()).map(entry => {
            const row = {};
            (attributes || []).forEach(attr => {
              if (typeof attr === 'string') {
                const idx = groupItems.findIndex(g => typeof g === 'string' && g === attr);
                if (idx >= 0) {
                  row[attr] = entry.groupValues[idx];
                }
              } else if (Array.isArray(attr) && attr.length === 2) {
                const [expr, alias] = attr;
                if (expr && typeof expr === 'object' && expr.fn === 'COUNT') {
                  row[alias] = entry.count;
                } else {
                  const idx = groupItems.findIndex(g => JSON.stringify(g) === JSON.stringify(expr));
                  row[alias] = idx >= 0 ? entry.groupValues[idx] : undefined;
                }
              }
            });

            // 确保 group 的字符串字段可直接返回（如 status/type）
            groupItems.forEach((item, idx) => {
              if (typeof item === 'string' && row[item] === undefined) {
                row[item] = entry.groupValues[idx];
              }
            });
            if (row.count === undefined) row.count = entry.count;
            return row;
          });

          return typeof limit === 'number' ? rows.slice(0, limit) : rows;
        }

        return results.slice(0, limit);
      },
      findAndCountAll: async ({ where, offset, limit, order }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
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
        createdAt: { field: 'created_at' }
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

  // 创建模拟的 taskDailyStatistics 模型
  const createMockDailyStatsModel = Op => {
    return {
      findOrCreate: async ({ where, defaults }) => {
        const existing = dailyStatsData.find(s => s.date === where.date);
        if (existing) {
          return [existing, false];
        }
        const stat = {
          ...defaults,
          update: async function (updateData) {
            Object.assign(this, updateData);
            return this;
          }
        };
        dailyStatsData.push(stat);
        return [stat, true];
      },
      findAll: async ({ where, order } = {}) => {
        let results = dailyStatsData;
        if (where && where.date) {
          const gte = where.date[Op.gte];
          if (gte) {
            results = results.filter(s => s.date >= gte);
          }
        }
        return results;
      },
      findOne: async ({ where } = {}) => {
        if (where && where.date) {
          return dailyStatsData.find(s => s.date === where.date) || null;
        }
        return dailyStatsData[0] || null;
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

    const mockTaskModel = createMockTaskModel(Op);
    const mockDailyStatsModel = createMockDailyStatsModel(Op);

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
        }
      },
      getUserModel: () => mockUserModel,
      getAuthenticate: () => [mockAuthenticate.user, mockAuthenticate.admin],
      ...options
    };

    app.decorate('task', {
      options: taskOptions,
      models: { task: mockTaskModel, taskDailyStatistics: mockDailyStatsModel },
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
    dailyStatsData = [];
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

  describe('updateDailyStatistics 测试', () => {
    it('should create daily statistics record when first task completes', async () => {
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

      // updateDailyStatistics 是异步的，等待其执行
      await new Promise(resolve => setTimeout(resolve, 100));

      expect(dailyStatsData.length).to.equal(1);
      const stat = dailyStatsData[0];
      expect(stat.totalCompleted).to.equal(1);
      expect(stat.successCount).to.equal(1);
      expect(stat.failedCount).to.equal(0);
      expect(stat.canceledCount).to.equal(0);
    });

    it('should increment existing daily statistics record', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task1 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });
      await fastify.task.services.complete({
        id: task1.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'manual'
      });
      await fastify.task.services.complete({
        id: task2.id,
        status: 'failed',
        error: 'error',
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      expect(dailyStatsData.length).to.equal(1);
      const stat = dailyStatsData[0];
      expect(stat.totalCompleted).to.equal(2);
      expect(stat.successCount).to.equal(1);
      expect(stat.failedCount).to.equal(1);
    });

    it('should update canceled count when task is canceled', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      await fastify.task.services.cancel({ id: created.id });

      await new Promise(resolve => setTimeout(resolve, 100));

      expect(dailyStatsData.length).to.equal(1);
      const stat = dailyStatsData[0];
      expect(stat.canceledCount).to.equal(1);
      expect(stat.totalCompleted).to.equal(1);
    });

    it('should calculate timing data when startedAt is set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      // 模拟任务已经启动，等待时间确保有毫秒差
      await new Promise(resolve => setTimeout(resolve, 10));
      const startedAt = new Date();
      await created.update({ startedAt });

      // 再等待一些时间确保执行时间有毫秒差
      await new Promise(resolve => setTimeout(resolve, 10));
      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      const stat = dailyStatsData[0];
      expect(stat.timedTaskCount).to.be.greaterThan(0);
      expect(stat.totalWaitingTime).to.be.greaterThan(0);
      expect(stat.totalExecutionTime).to.be.greaterThan(0);
      expect(stat.totalTime).to.be.greaterThan(0);
    });

    it('should use createdAt as startedAt fallback when startedAt is not set', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      // 不设置 startedAt，直接完成
      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      const task = await fastify.task.services.detail({ id: created.id });
      // complete 方法内部会设置 startedAt = task.startedAt || task.createdAt
      expect(task.startedAt).to.exist;
    });

    it('should track byType statistics with timing data', async () => {
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

      await new Promise(resolve => setTimeout(resolve, 100));

      const stat = dailyStatsData[0];
      expect(stat.byType).to.exist;
      expect(stat.byType['test-type']).to.exist;
      expect(stat.byType['test-type'].count).to.equal(1);
      expect(stat.byType['test-type'].successCount).to.equal(1);
    });

    it('should track byRunnerType statistics', async () => {
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

      await new Promise(resolve => setTimeout(resolve, 100));

      const stat = dailyStatsData[0];
      expect(stat.byRunnerType).to.exist;
      expect(stat.byRunnerType['manual']).to.exist;
      expect(stat.byRunnerType['manual'].count).to.equal(1);
    });

    it('should not throw when updateDailyStatistics encounters an error', async () => {
      fastify = await createFastify();
      await fastify.ready();

      // 模拟 taskDailyStatistics.findOrCreate 抛出异常
      fastify.task.models.taskDailyStatistics.findOrCreate = async () => {
        throw new Error('DB connection error');
      };

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      // 不应该抛出异常，因为 updateDailyStatistics 是异步的并内部捕获了错误
      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      await new Promise(resolve => setTimeout(resolve, 100));

      // 应该记录了错误日志
      expect(fastify.log.error.called).to.be.true;
    });
  });

  describe('statistics.getOverview 测试', () => {
    it('should return overview with default 7d range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getOverview({});

      expect(result.range).to.equal('7d');
      expect(result.rangeLabel).to.equal('近7天');
      expect(result.totalTasks).to.exist;
      expect(result.byStatus).to.exist;
      expect(result.byType).to.exist;
      expect(result.byRunnerType).to.exist;
      expect(result.byTargetType).to.exist;
      expect(result.recentTrend).to.exist;
      expect(result.recentTrendByStatus).to.exist;
      expect(result.recentTrendByType).to.exist;
      expect(result.durationTrend).to.exist;
    });

    it('should normalize invalid range to 7d', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getOverview({ range: 'invalid' });

      expect(result.range).to.equal('7d');
      expect(result.rangeLabel).to.equal('近7天');
    });

    it('should support 1m range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getOverview({ range: '1m' });

      expect(result.range).to.equal('1m');
      expect(result.rangeLabel).to.equal('近1个月');
    });

    it('should support 1y range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getOverview({ range: '1y' });

      expect(result.range).to.equal('1y');
      expect(result.rangeLabel).to.equal('近1年');
    });

    it('should return durationTrend from daily statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const today = new Date().toISOString().split('T')[0];
      dailyStatsData.push({
        date: today,
        totalCompleted: 5,
        successCount: 3,
        failedCount: 1,
        canceledCount: 1,
        totalWaitingTime: 10000,
        totalExecutionTime: 20000,
        totalTime: 30000,
        timedTaskCount: 5,
        byType: {
          'test-type': {
            count: 5,
            successCount: 3,
            failedCount: 1,
            canceledCount: 1,
            totalWaitingTime: 10000,
            totalExecutionTime: 20000,
            totalTime: 30000,
            timedTaskCount: 5
          }
        },
        byRunnerType: {
          manual: {
            count: 5,
            successCount: 3,
            failedCount: 1,
            canceledCount: 1,
            totalWaitingTime: 10000,
            totalExecutionTime: 20000,
            totalTime: 30000,
            timedTaskCount: 5
          }
        }
      });

      const result = await fastify.task.services.statistics.getOverview({ range: '7d' });

      expect(result.durationTrend).to.have.length.greaterThan(0);
      const todayStat = result.durationTrend.find(d => d.date === today);
      expect(todayStat).to.exist;
      expect(todayStat.completedCount).to.equal(5);
      expect(todayStat.successCount).to.equal(3);
      expect(todayStat.failedCount).to.equal(1);
      expect(todayStat.canceledCount).to.equal(1);
      expect(todayStat.avgWaitingTime).to.equal(2000);
      expect(todayStat.avgExecutionTime).to.equal(4000);
      expect(todayStat.avgTotalTime).to.equal(6000);
    });

    it('should return empty durationTrend when no daily stats exist', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getOverview({ range: '7d' });

      expect(result.durationTrend).to.deep.equal([]);
    });

    it('should return byType/byRunnerType with avg timing in durationTrend', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const today = new Date().toISOString().split('T')[0];
      dailyStatsData.push({
        date: today,
        totalCompleted: 3,
        successCount: 2,
        failedCount: 1,
        canceledCount: 0,
        totalWaitingTime: 6000,
        totalExecutionTime: 12000,
        totalTime: 18000,
        timedTaskCount: 3,
        byType: {
          'test-type': {
            count: 3,
            successCount: 2,
            failedCount: 1,
            canceledCount: 0,
            totalWaitingTime: 6000,
            totalExecutionTime: 12000,
            totalTime: 18000,
            timedTaskCount: 3
          }
        },
        byRunnerType: {
          system: {
            count: 3,
            successCount: 2,
            failedCount: 1,
            canceledCount: 0,
            totalWaitingTime: 6000,
            totalExecutionTime: 12000,
            totalTime: 18000,
            timedTaskCount: 3
          }
        }
      });

      const result = await fastify.task.services.statistics.getOverview({ range: '7d' });

      const todayStat = result.durationTrend.find(d => d.date === today);
      expect(todayStat.byType['test-type'].count).to.equal(3);
      expect(todayStat.byType['test-type'].avgWaitingTime).to.equal(2000);
      expect(todayStat.byType['test-type'].avgExecutionTime).to.equal(4000);
      expect(todayStat.byType['test-type'].avgTotalTime).to.equal(6000);
      expect(todayStat.byRunnerType['system'].count).to.equal(3);
      expect(todayStat.byRunnerType['system'].avgTotalTime).to.equal(6000);
    });

    it('should handle daily stats with zero timedTaskCount', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const today = new Date().toISOString().split('T')[0];
      dailyStatsData.push({
        date: today,
        totalCompleted: 2,
        successCount: 2,
        failedCount: 0,
        canceledCount: 0,
        totalWaitingTime: 0,
        totalExecutionTime: 0,
        totalTime: 0,
        timedTaskCount: 0,
        byType: {},
        byRunnerType: {}
      });

      const result = await fastify.task.services.statistics.getOverview({ range: '7d' });

      const todayStat = result.durationTrend.find(d => d.date === today);
      expect(todayStat).to.exist;
      expect(todayStat.avgWaitingTime).to.equal(0);
      expect(todayStat.avgExecutionTime).to.equal(0);
      expect(todayStat.avgTotalTime).to.equal(0);
    });
  });

  describe('statistics.getRealtime 测试', () => {
    it('should return realtime statistics with today data', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getRealtime({});

      expect(result.date).to.exist;
      expect(result.totalTasks).to.exist;
      expect(result.byStatus).to.exist;
      expect(result.byType).to.exist;
      expect(result.byRunnerType).to.exist;
      expect(result.runnerTypeStats).to.exist;
      expect(result.hourlyTrend).to.exist;
      expect(result.hourlyTrendByStatus).to.exist;
      expect(result.intervalTrend).to.exist;
      expect(result.todayDuration).to.exist;
    });

    it('should return todayDuration with zero values when no daily stats', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getRealtime({});

      expect(result.todayDuration.completedCount).to.equal(0);
      expect(result.todayDuration.avgWaitingTime).to.equal(0);
      expect(result.todayDuration.avgExecutionTime).to.equal(0);
      expect(result.todayDuration.avgTotalTime).to.equal(0);
      expect(result.todayDuration.byType).to.deep.equal({});
      expect(result.todayDuration.byRunnerType).to.deep.equal({});
    });

    it('should fall back to today task aggregates when daily statistics are missing', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const now = new Date();
      const created = new Date(now.getTime() - 15000);
      const started = new Date(now.getTime() - 10000);

      await fastify.task.models.task.create({
        type: 'import',
        status: 'success',
        runnerType: 'manual',
        targetId: 'dur-fb-1',
        targetType: 'doc',
        createdAt: created,
        startedAt: started,
        completedAt: now
      });

      const result = await fastify.task.services.statistics.getRealtime({});

      expect(result.todayDuration.completedCount).to.equal(1);
      expect(result.todayDuration.avgTotalTime).to.be.greaterThan(0);
      expect(result.todayDuration.byType.import.count).to.equal(1);
      expect(result.todayDuration.byType.import.avgTotalTime).to.be.greaterThan(0);
      expect(result.todayDuration.byRunnerType.manual.count).to.equal(1);
    });

    it('should return todayDuration from daily statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const tmp = await fastify.task.services.statistics.getRealtime({});
      const today = tmp.date;
      dailyStatsData.push({
        date: today,
        totalCompleted: 10,
        successCount: 8,
        failedCount: 1,
        canceledCount: 1,
        totalWaitingTime: 50000,
        totalExecutionTime: 100000,
        totalTime: 150000,
        timedTaskCount: 10,
        byType: {
          'test-type': {
            count: 10,
            successCount: 8,
            failedCount: 1,
            canceledCount: 1,
            totalWaitingTime: 50000,
            totalExecutionTime: 100000,
            totalTime: 150000,
            timedTaskCount: 10
          }
        },
        byRunnerType: {
          manual: {
            count: 10,
            successCount: 8,
            failedCount: 1,
            canceledCount: 1,
            totalWaitingTime: 50000,
            totalExecutionTime: 100000,
            totalTime: 150000,
            timedTaskCount: 10
          }
        }
      });

      const result = await fastify.task.services.statistics.getRealtime({});

      expect(result.todayDuration.completedCount).to.equal(10);
      expect(result.todayDuration.successCount).to.equal(8);
      expect(result.todayDuration.failedCount).to.equal(1);
      expect(result.todayDuration.canceledCount).to.equal(1);
      expect(result.todayDuration.avgWaitingTime).to.equal(5000);
      expect(result.todayDuration.avgExecutionTime).to.equal(10000);
      expect(result.todayDuration.avgTotalTime).to.equal(15000);
      expect(result.todayDuration.byType['test-type'].count).to.equal(10);
      expect(result.todayDuration.byType['test-type'].avgWaitingTime).to.equal(5000);
      expect(result.todayDuration.byRunnerType['manual'].count).to.equal(10);
      expect(result.todayDuration.byRunnerType['manual'].avgExecutionTime).to.equal(10000);
    });

    it('should aggregate hourlyTrendByType and hourlyTrendByStatus correctly', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const now = new Date();
      const h = now.getHours();
      const createdAtHour = new Date(now);
      createdAtHour.setHours(h, 10, 0, 0);

      await fastify.task.models.task.create({
        type: 'import',
        status: 'success',
        runnerType: 'manual',
        targetId: 't-1',
        targetType: 'doc',
        createdAt: createdAtHour
      });
      await fastify.task.models.task.create({
        type: 'import',
        status: 'running',
        runnerType: 'manual',
        targetId: 't-2',
        targetType: 'doc',
        createdAt: createdAtHour
      });
      await fastify.task.models.task.create({
        type: 'sync',
        status: 'waiting',
        runnerType: 'system',
        targetId: 't-3',
        targetType: 'doc',
        createdAt: createdAtHour
      });

      const result = await fastify.task.services.statistics.getRealtime({});

      const importTotal = result.hourlyTrendByType
        .filter(item => item.type === 'import')
        .reduce((sum, item) => sum + Number(item.count || 0), 0);
      const syncTotal = result.hourlyTrendByType
        .filter(item => item.type === 'sync')
        .reduce((sum, item) => sum + Number(item.count || 0), 0);
      const successTotal = result.hourlyTrendByStatus
        .filter(item => item.status === 'success')
        .reduce((sum, item) => sum + Number(item.count || 0), 0);
      const runningTotal = result.hourlyTrendByStatus
        .filter(item => item.status === 'running')
        .reduce((sum, item) => sum + Number(item.count || 0), 0);
      const waitingTotal = result.hourlyTrendByStatus
        .filter(item => item.status === 'waiting')
        .reduce((sum, item) => sum + Number(item.count || 0), 0);

      expect(importTotal).to.equal(2);
      expect(syncTotal).to.equal(1);
      expect(successTotal).to.equal(1);
      expect(runningTotal).to.equal(1);
      expect(waitingTotal).to.equal(1);
    });

    it('should apply type and runnerType filters in realtime statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const now = new Date();
      await fastify.task.models.task.create({
        type: 'import',
        status: 'success',
        runnerType: 'manual',
        targetId: 'a-1',
        targetType: 'doc',
        createdAt: now
      });
      await fastify.task.models.task.create({
        type: 'sync',
        status: 'success',
        runnerType: 'manual',
        targetId: 'a-2',
        targetType: 'doc',
        createdAt: now
      });
      await fastify.task.models.task.create({
        type: 'import',
        status: 'running',
        runnerType: 'system',
        targetId: 'a-3',
        targetType: 'doc',
        createdAt: now
      });

      const result = await fastify.task.services.statistics.getRealtime({ type: 'import', runnerType: 'manual' });

      expect(result.totalTasks).to.equal(1);
      expect(result.byType.import).to.equal(1);
      expect(result.byType.sync).to.equal(undefined);
      expect(result.byRunnerType.manual).to.equal(1);
      expect(result.byRunnerType.system).to.equal(undefined);
      expect(result.byStatus.success).to.equal(1);
      expect(result.runnerTypeStats.manual).to.deep.equal({ total: 1, pending: 0, executed: 1 });
    });

    it('should return runnerTypeStats with pending and executed counts per runnerType', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const now = new Date();
      await fastify.task.models.task.create({
        type: 'import',
        status: 'pending',
        runnerType: 'manual',
        targetId: 'rts-1',
        targetType: 'doc',
        createdAt: now
      });
      await fastify.task.models.task.create({
        type: 'import',
        status: 'success',
        runnerType: 'manual',
        targetId: 'rts-2',
        targetType: 'doc',
        createdAt: now
      });
      await fastify.task.models.task.create({
        type: 'import',
        status: 'running',
        runnerType: 'manual',
        targetId: 'rts-3',
        targetType: 'doc',
        createdAt: now
      });
      await fastify.task.models.task.create({
        type: 'sync',
        status: 'pending',
        runnerType: 'system',
        targetId: 'rts-4',
        targetType: 'doc',
        createdAt: now
      });

      const result = await fastify.task.services.statistics.getRealtime({});

      expect(result.runnerTypeStats.manual).to.deep.equal({ total: 3, pending: 1, executed: 2 });
      expect(result.runnerTypeStats.system).to.deep.equal({ total: 1, pending: 1, executed: 0 });
    });

    it('should format realtime date with timezone parameter', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.statistics.getRealtime({ timezone: 'UTC' });
      const expected = dayjs().tz('UTC').format('YYYY-MM-DD');
      expect(result.date).to.equal(expected);
    });
  });

  describe('统计接口路由测试', () => {
    it('should register statistics route', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics'
      });
      expect(response.statusCode).to.not.equal(404);
    });

    it('should register statistics SSE route', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse'
      });
      expect(response.statusCode).to.not.equal(404);
    });

    it('should expose statistics service', async () => {
      fastify = await createFastify();
      await fastify.ready();

      expect(fastify.task.services.statistics).to.exist;
      expect(fastify.task.services.statistics.getOverview).to.be.a('function');
      expect(fastify.task.services.statistics.getRealtime).to.be.a('function');
    });

    it('should pass timezone and type query to statistics overview', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const overviewStub = sinon.stub(fastify.task.services.statistics, 'getOverview').resolves({
        range: '7d',
        rangeLabel: '近7天',
        totalTasks: 0,
        byStatus: {},
        byType: {},
        byRunnerType: {},
        byTargetType: {},
        recentTrend: [],
        recentTrendByStatus: [],
        recentTrendByType: [],
        durationTrend: []
      });

      const res = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics?range=7d&timezone=UTC&type=import'
      });
      expect(res.statusCode).to.equal(200);
      expect(overviewStub.calledOnce).to.be.true;
      expect(overviewStub.firstCall.args[0]).to.deep.equal({
        range: '7d',
        timezone: 'UTC',
        type: 'import'
      });
    });

    it('should pass timezone and type query to statistics sse', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const res = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse?interval=1&timezone=UTC&type=import'
      });
      expect(res.statusCode).to.not.equal(404);
      // inject 场景下不稳定地触发 sse 流，仅校验路由可正确匹配并接受查询参数
      expect(res.statusCode).to.not.equal(400);
    });
  });
});

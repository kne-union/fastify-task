const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - registration and extension options', function () {
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
      expect(services.claimPendingTasks).to.exist;
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
});

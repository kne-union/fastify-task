const { expect } = require('chai');
const sinon = require('sinon');
const { createFastify } = require('./helpers/setup');

describe('@kne/fastify-task - 升级与高级功能', function () {
  this.timeout(10000);
  let fastify;
  let taskData = [];
  let taskIdCounter = { value: 1 };
  beforeEach(() => { taskData = []; taskIdCounter.value = 1; });
  afterEach(async () => { if (fastify) { await fastify.close(); fastify = null; } sinon.restore(); });

  describe('startedAt 字段测试', () => {
    it('should set startedAt when complete is called', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      expect(callArgs.channel).to.match(/^test-type:manual:\d+$/);
      expect(callArgs.data.total).to.equal(1);
      expect(callArgs.data.success).to.equal(1);
      expect(callArgs.time).to.exist;
    });

    it('should call collect with failed status when task fails', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      expect(callArgs.channel).to.match(/^test-type:system:\d+$/);
      expect(callArgs.data.failed).to.equal(1);
    });

    it('should call collect with canceled status when task is canceled', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d' }
      });
      expect(response.statusCode).to.equal(200);
      const body = JSON.parse(response.payload);
      expect(body).to.have.property('totalTasks');
      expect(body).to.have.property('byStatus');
      expect(body).to.have.property('byType');
    });

    it('should return durationTrend in statistics response', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d' }
      });
      expect(response.statusCode).to.equal(200);
      const body = JSON.parse(response.payload);
      expect(body).to.have.property('durationTrend');
      expect(Array.isArray(body.durationTrend)).to.be.true;
    });

    it('should return statistics with type filter', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d', type: 'test-type' }
      });
      expect(response.statusCode).to.equal(200);
      const body = JSON.parse(response.payload);
      expect(body).to.have.property('totalTasks');
      expect(body).to.have.property('durationTrend');
    });

    it('should return statistics with type and runnerType filter', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics',
        query: { range: '7d', type: 'test-type', runnerType: 'manual' }
      });
      expect(response.statusCode).to.equal(200);
      const body = JSON.parse(response.payload);
      expect(body).to.have.property('totalTasks');
      expect(body).to.have.property('durationTrend');
    });

    it('should throw error for unsupported range', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const result = await fastify.task.services.append({
        dirs: ['/path/to/dir1', '/path/to/dir2']
      });

      expect(result.dirs).to.deep.equal(['/path/to/dir1', '/path/to/dir2']);
      expect(fastify.task.options.dirs).to.include('/path/to/dir1');
      expect(fastify.task.options.dirs).to.include('/path/to/dir2');
    });

    it('should skip existing dirs when appending', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const existingDir = fastify.task.options.dirs[0];
      const result = await fastify.task.services.append({
        dirs: [existingDir, '/path/to/new-dir']
      });

      expect(result.dirs).to.deep.equal(['/path/to/new-dir']);
    });

    it('should append new tasks', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      expect(fastify.task.options.dirs).to.be.an('array');
      expect(fastify.task.options.dirs[0]).to.equal(fastify.task.options.dir);
    });

    it('should merge dir into dirs when dirs is provided without dir', async () => {
      const customDir = '/custom/tasks';
      fastify = await createFastify({ dirs: [customDir] }, taskData, taskIdCounter);
      await fastify.ready();

      expect(fastify.task.options.dirs).to.include(fastify.task.options.dir);
      expect(fastify.task.options.dirs).to.include(customDir);
    });
  });

  describe('升级功能测试 - 任务优先级 (#1)', () => {
    it('should create task with priority', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task.priority).to.equal(0);
    });

    it('should throw error for non-integer priority', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
        expect(e.message).to.include('参数必须为整数');
      }
    });
  });

  describe('升级功能测试 - 任务依赖 (#2)', () => {
    it('should create task with parentTaskId', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
        expect(e.message).to.include('参数必须在 0 到 10 之间');
      }
    });

    it('should reset retryCount when retry is called', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '1m' });
      expect(result).to.exist;

      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.startTime).to.exist;
      expect(callArgs.endTime).to.exist;
    });

    it('should handle 3m range', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '3m' });
      expect(result).to.exist;
    });

    it('should handle 1y range', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '1y' });
      expect(result).to.exist;
    });

    it('should handle 7d range explicitly', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
    });

    it('should throw error for invalid range', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      try {
        await fastify.task.services.queryStatistics({ range: '1w' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('不支持的时间范围');
      }
    });

    it('should pass timezone to query', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.task.services.queryStatistics({ range: '7d', timezone: 'Asia/Shanghai' });

      const callArgs = fastify.taskStatistics.services.query.firstCall.args[0];
      expect(callArgs.timezone).to.equal('Asia/Shanghai');
    });
  });
});

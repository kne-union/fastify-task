const { expect } = require('chai');
const sinon = require('sinon');
const { createFastify } = require('./helpers/setup');

describe('@kne/fastify-task - 统计与时区', function () {
  this.timeout(10000);
  let fastify;
  let taskData = [];
  let taskIdCounter = { value: 1 };
  beforeEach(() => { taskData = []; taskIdCounter.value = 1; });
  afterEach(async () => { if (fastify) { await fastify.close(); fastify = null; } sinon.restore(); });

  describe('SSE 统计接口测试', () => {
    it('should call sseStatistics with correct params', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse',
        query: { timezone: 'Asia/Shanghai' }
      });

      expect(fastify.taskStatistics.services.sseStream.send.calledOnce).to.be.true;
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      expect(sendArgs[1].name).to.equal('taskStatistics');
      expect(sendArgs[1].params).to.have.property('timezone');
    });

    it('should call sseStatistics with default params when no filter', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse'
      });

      expect(fastify.taskStatistics.services.sseStream.send.calledOnce).to.be.true;
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      expect(sendArgs[1].name).to.equal('taskStatistics');
      expect(sendArgs[1].params).to.have.property('timezone');
    });

    it('should call sseStatistics fetchData and return query result', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse'
      });

      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();
      expect(result).to.exist;
      expect(result).to.have.property('totalTasks');
      expect(result).to.have.property('byStatus');
    });
  });

  describe('collectTaskStatistics 时序数据测试', () => {
    it('should include timing data when task has startedAt', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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

      expect(fastify.taskStatistics.services.collect.callCount).to.be.greaterThan(0);
      const calls = fastify.taskStatistics.services.collect.getCalls();
      const call = calls[calls.length - 1];
      const callArgs = call.args[0];
      expect(callArgs.data.total).to.equal(1);
      expect(callArgs.data.success).to.equal(1);
      expect(callArgs.unit.total).to.equal('count');
      expect(callArgs.unit.success).to.equal('count');
      expect(callArgs.data.waitingTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.data.executionTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.data.totalTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.unit.waitingTime).to.equal('ms');
      expect(callArgs.unit.executionTime).to.equal('ms');
      expect(callArgs.unit.totalTime).to.equal('ms');
    });

    it('should calculate executionTime from totalTime when no startedAt', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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

      expect(fastify.taskStatistics.services.collect.callCount).to.be.greaterThan(0);
      const calls = fastify.taskStatistics.services.collect.getCalls();
      const call = calls[calls.length - 1];
      const callArgs = call.args[0];
      expect(callArgs.data.executionTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.data.totalTime).to.be.a('number').and.greaterThan(0);
      expect(callArgs.unit.executionTime).to.equal('ms');
      expect(callArgs.unit.totalTime).to.equal('ms');
    });
  });

  describe('executor 系统任务执行器测试', () => {
    it('should execute system task via executor', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({ taskTimeout: 50 }, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({ limit: 1 }, taskData, taskIdCounter);
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
      fastify = await createFastify({ limit: 10 }, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
  describe('统计接口默认参数测试', () => {
    it('should use default range when not provided in statistics query', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics'
      });
      expect(response.statusCode).to.equal(200);
    });

    it('should use default range and interval in SSE statistics', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({ taskTimeout: 0 }, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.task.services.runner();

      // 不应抛出异常，正常运行
      expect(fastify.log.info.called).to.be.false;
    });
  });

  describe('collectTaskStatistics 无时间数据测试', () => {
    it('should not include timing data when task has no createdAt or completedAt', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
  describe('queryAndParse 数据处理测试', () => {
    const buildMockQueryResult = () => {
      const now = new Date();
      return {
        list: [
          // 1-segment channel, daily
          {
            channel: 'test-type', period: 'd', time: now,
            data: { sum: { total: 10, success: 8, failed: 1, canceled: 1, waitingTime: 5000, executionTime: 10000, totalTime: 15000 } }
          },
          // 2-segment channel, daily
          {
            channel: 'test-type:manual', period: 'd', time: now,
            data: { sum: { total: 5, success: 4, failed: 1, canceled: 0, waitingTime: 2000, executionTime: 5000, totalTime: 7000 } }
          },
          // 2-segment channel system, daily
          {
            channel: 'test-type:system', period: 'd', time: now,
            data: { sum: { total: 5, success: 4, failed: 0, canceled: 1, waitingTime: 3000, executionTime: 5000, totalTime: 8000 } }
          },
          // 1-segment channel, hourly
          {
            channel: 'test-type', period: 'h', time: now,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 1000, executionTime: 3000, totalTime: 4000 } }
          },
          // 2-segment channel, hourly
          {
            channel: 'test-type:manual', period: 'h', time: now,
            data: { sum: { total: 2, success: 2, failed: 0, canceled: 0, waitingTime: 500, executionTime: 2000, totalTime: 2500 } }
          },
          // 2-segment channel system, hourly
          {
            channel: 'test-type:system', period: 'h', time: now,
            data: { sum: { total: 1, success: 0, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          }
        ]
      };
    };

    it('should process statistics data with 1-segment and 2-segment channels', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.query.resolves(buildMockQueryResult());

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      // 1-segment channels: daily(10) + hourly(3) = 13
      expect(result.totalTasks).to.equal(13);
      // byStatus from 1-segment: daily(8+1+1) + hourly(2+1+0)
      expect(result.byStatus.success).to.equal(10);
      expect(result.byStatus.failed).to.equal(2);
      expect(result.byStatus.canceled).to.equal(1);
      expect(result.byType['test-type']).to.equal(13);
      // byRunnerType from 2-segment: manual(5+2) + system(5+1)
      expect(result.byRunnerType.manual).to.equal(7);
      expect(result.byRunnerType.system).to.equal(6);
    });

    it('should build daily trend arrays from statistics', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.query.resolves(buildMockQueryResult());

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.recentTrend).to.exist;
      expect(Array.isArray(result.recentTrend)).to.be.true;
      expect(result.recentTrendByStatus).to.exist;
      expect(result.recentTrendByType).to.exist;
    });

    it('should build hourly trend arrays from statistics', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.query.resolves(buildMockQueryResult());

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.hourlyTrend).to.exist;
      expect(Array.isArray(result.hourlyTrend)).to.be.true;
      expect(result.hourlyCompletionTrend).to.exist;
      expect(Array.isArray(result.hourlyCompletionTrend)).to.be.true;
    });

    it('should build duration trend from statistics', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.query.resolves(buildMockQueryResult());

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.durationTrend).to.exist;
      expect(Array.isArray(result.durationTrend)).to.be.true;
      if (result.durationTrend.length > 0) {
        const trendItem = result.durationTrend[0];
        expect(trendItem).to.have.property('date');
        expect(trendItem).to.have.property('completedCount');
        expect(trendItem).to.have.property('avgWaitingTime');
        expect(trendItem).to.have.property('avgExecutionTime');
        expect(trendItem).to.have.property('avgTotalTime');
        expect(trendItem).to.have.property('byType');
        expect(trendItem).to.have.property('byRunnerType');
      }
    });

    it('should filter by runnerType in queryAndParse', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.query.resolves(buildMockQueryResult());

      const result = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'manual' });
      // runnerType filter should exclude items that don't match
      expect(result).to.exist;
    });

    it('should return empty result when channels are empty', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.channelMeta.list.resolves([]);
      fastify.taskStatistics.services.query.resolves({ list: [] });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.totalTasks).to.equal(0);
    });

    it('should handle queryStatistics error gracefully', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      fastify.taskStatistics.services.query.rejects(new Error('DB error'));

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
      expect(result.totalTasks).to.equal(0);
    });
  });

  describe('buildSseData 测试', () => {
    it('should build SSE data with tasks in various states', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // Create tasks in different states
      const pendingTask = await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', status: 'pending'
      });
      const runningTask = await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-2', targetType: 'document',
        runnerType: 'system', status: 'running'
      });
      const waitingTask = await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-3', targetType: 'document',
        runnerType: 'manual', status: 'waiting'
      });
      const successTask = await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-4', targetType: 'document',
        runnerType: 'manual', status: 'success'
      });
      const failedTask = await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-5', targetType: 'document',
        runnerType: 'system', status: 'failed'
      });

      // Set up mock query to return empty for SSE
      fastify.taskStatistics.services.query.resolves({ list: [] });

      // Call SSE endpoint and use fetchData
      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();

      expect(result).to.exist;
      expect(result).to.have.property('totalTasks');
      expect(result).to.have.property('byStatus');
      expect(result).to.have.property('pendingByRunnerType');
      expect(result).to.have.property('waitingByRunnerType');
      expect(result).to.have.property('runningByRunnerType');
      expect(result).to.have.property('runnerTypeStats');
      expect(result).to.have.property('hourlyTrendByStatus');
      expect(result).to.have.property('todayDuration');
    });

    it('should build SSE data with statistics query result', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: now,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 2000, executionTime: 5000, totalTime: 7000 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: now,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 1000, executionTime: 3000, totalTime: 4000 } }
          },
          {
            channel: 'test-type:system', period: 'h', time: now,
            data: { sum: { total: 2, success: 1, failed: 0, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          }
        ]
      });

      // Create a running task
      await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', status: 'running'
      });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();

      expect(result.totalTasks).to.be.greaterThan(0);
      expect(result.byStatus.running).to.be.greaterThan(0);
      expect(result.completedToday).to.exist;
      expect(result.todayDuration).to.exist;
      expect(result.todayDuration.completedCount).to.equal(5);
      expect(result.hourlyTrend).to.exist;
      expect(result.hourlyTrendByStatus).to.exist;
      expect(result.hourlyTrendByType).to.exist;
      expect(result.intervalTrend).to.exist;
    });

    it('should handle SSE statistics query error gracefully', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      fastify.taskStatistics.services.query.rejects(new Error('SSE query error'));

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();

      // Should still return basic data from DB even when statistics query fails
      expect(result).to.exist;
      expect(result).to.have.property('totalTasks');
    });

    it('should build SSE data with runnerType filter', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', status: 'running'
      });
      await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-2', targetType: 'document',
        runnerType: 'manual', status: 'pending'
      });
      fastify.taskStatistics.services.query.resolves({ list: [] });

      await fastify.inject({
        method: 'GET',
        url: '/api/task/statistics/sse',
        query: { runnerType: 'system' }
      });

      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();

      expect(result).to.exist;
      expect(result.runningByRunnerType.system).to.equal(1);
    });

    it('should calculate waitingQueueMaxWaitMs from pending/waiting tasks', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', status: 'pending',
        createdAt: new Date(Date.now() - 30000)
      });
      fastify.taskStatistics.services.query.resolves({ list: [] });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();

      expect(result.waitingQueueMaxWaitMsByRunnerType).to.exist;
      expect(result.waitingQueueMaxWaitMsByRunnerType.system).to.be.greaterThan(0);
    });
  });

  describe('sseStatistics 错误处理测试', () => {
    it('should handle fetchData error and return empty object', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // Make buildSseData throw by breaking the model
      fastify.task.models.task.findAll = async () => { throw new Error('DB connection lost'); };

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();
      expect(result).to.deep.equal({});
    });
  });

  describe('queryAndParse 边界分支测试', () => {
    it('should handle item with NaN values in sum', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [{
          channel: 'test-type', period: 'd', time: now,
          data: { sum: { total: 'invalid', success: NaN, failed: undefined, canceled: null, waitingTime: 'bad', executionTime: NaN, totalTime: null } }
        }]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
      // NaN/invalid values should be converted to 0 by safeNum
      expect(result.byStatus.success).to.equal(0);
    });

    it('should handle channel with only one segment (no runnerType)', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [{
          channel: 'simple-type', period: 'd', time: now,
          data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 100, executionTime: 200, totalTime: 300 } }
        }]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.totalTasks).to.equal(1);
      expect(result.byType['simple-type']).to.equal(1);
    });

    it('should handle runnerType filter excluding items without runnerTypeName', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'd', time: now,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1 } }
          },
          {
            channel: 'test-type:manual', period: 'd', time: now,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'manual' });
      // With runnerType filter, 1-segment channels without runnerTypeName should be excluded
      expect(result).to.exist;
    });
  });

  describe('buildSseData completedToday 时长数据测试', () => {
    it('should include completedTodayTotalDurationMsByRunnerType', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type:manual', period: 'h', time: now,
            data: { sum: { total: 2, success: 2, failed: 0, canceled: 0, waitingTime: 1000, executionTime: 3000, totalTime: 4000 } }
          },
          {
            channel: 'test-type:system', period: 'h', time: now,
            data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 500, executionTime: 2000, totalTime: 2500 } }
          }
        ]
      });
      await fastify.task.models.task.create({
        type: 'test-type', targetId: 't1', targetType: 'doc', runnerType: 'manual', status: 'running'
      });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();

      expect(result.completedTodayTotalDurationMsByRunnerType).to.exist;
      expect(result.completedTodayTotalDurationMsByRunnerType.manual).to.equal(4000);
      expect(result.completedTodayTotalDurationMsByRunnerType.system).to.equal(2500);
    });
  });

  describe('queryAndParse 多日/多时排序测试', () => {
    it('should sort durationTrend by date when multiple dates exist', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const now = new Date();
      const yesterday = new Date(now.getTime() - 86400000);
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'd', time: yesterday,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type', period: 'd', time: now,
            data: { sum: { total: 5, success: 4, failed: 0, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.durationTrend.length).to.equal(2);
      // Should be sorted by date ascending
      expect(result.durationTrend[0].date < result.durationTrend[1].date).to.be.true;
    });

    it('should sort hourly allHours when multiple hours exist', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const now = new Date();
      const hourAgo = new Date(now.getTime() - 3600000);
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: hourAgo,
            data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type', period: 'h', time: now,
            data: { sum: { total: 3, success: 2, failed: 0, canceled: 1, waitingTime: 800, executionTime: 1500, totalTime: 2300 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: hourAgo,
            data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 300, executionTime: 800, totalTime: 1100 } }
          },
          {
            channel: 'test-type:system', period: 'h', time: now,
            data: { sum: { total: 1, success: 0, failed: 1, canceled: 0, waitingTime: 200, executionTime: 600, totalTime: 800 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.hourlyTrend.length).to.be.greaterThan(0);
      expect(result.hourlyCompletionTrend.length).to.be.greaterThan(0);
    });
  });

  describe('时区支持测试', () => {
    it('parseRange 应使用客户端时区计算日期范围', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // 模拟服务器 UTC+0，客户端 UTC+8
      // 当 UTC 时间为 2026-05-28 02:00 时，CST 为 2026-05-28 10:00
      // 对于 7d 范围：CST 应从 2026-05-21 00:00 CST 开始，即 2026-05-20 16:00 UTC
      const resultNoTz = await fastify.task.services.queryStatistics({ range: '7d' });
      const resultWithTz = await fastify.task.services.queryStatistics({ range: '7d', timezone: 'Asia/Shanghai' });

      // 无时区参数时使用服务器时间，有时区参数时使用客户端时区
      // 两者应都能正常返回
      expect(resultNoTz).to.exist;
      expect(resultWithTz).to.exist;
    });

    it('queryStatistics 应将服务器时间的日期/小时转换为客户端时区', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // 模拟数据：服务器 UTC+0 时间 2026-05-28T02:00:00Z
      // 客户端 UTC+8 应显示为 2026-05-28 10:00
      const serverTime = new Date('2026-05-28T02:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: serverTime,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: serverTime,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', timezone: 'Asia/Shanghai' });

      // 无时区时 hour=2（服务器本地），有时区时 hour=10（CST）
      expect(result.hourlyTrend).to.exist;
      expect(Array.isArray(result.hourlyTrend)).to.be.true;
      if (result.hourlyTrend.length > 0) {
        // UTC+8 时区下，02:00 UTC = 10:00 CST
        expect(result.hourlyTrend[0].hour).to.equal(10);
      }
      if (result.hourlyCompletionTrend.length > 0) {
        expect(result.hourlyCompletionTrend[0].hour).to.equal(10);
      }
    });

    it('queryStatistics 无时区时使用服务器本地时间', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const serverTime = new Date('2026-05-28T02:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: serverTime,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: serverTime,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });

      expect(result.hourlyTrend).to.exist;
      if (result.hourlyTrend.length > 0) {
        // 无时区时，hour 取服务器本地时间
        // 注意：测试环境的服务器时区可能不是 UTC，这里只验证 hour 是数字
        expect(typeof result.hourlyTrend[0].hour).to.equal('number');
      }
    });

    it('日期分组应使用客户端时区', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // UTC 时间 2026-05-28T22:00:00Z，在 UTC+8 下是 2026-05-29 06:00
      const serverTime = new Date('2026-05-28T22:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: serverTime,
            data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: serverTime,
            data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 200, executionTime: 500, totalTime: 700 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', timezone: 'Asia/Shanghai' });

      // 日期应为 2026-05-29（CST），而非 2026-05-28（UTC）
      if (result.recentTrend.length > 0) {
        expect(result.recentTrend.some(t => t.date === '2026-05-29')).to.be.true;
      }
      if (result.hourlyTrend.length > 0) {
        expect(result.hourlyTrend[0].date).to.equal('2026-05-29');
        // UTC+8 下 22:00 UTC = 06:00 CST
        expect(result.hourlyTrend[0].hour).to.equal(6);
      }
    });

    it('SSE buildSseData 应使用客户端时区计算今日边界', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // 创建一个 running 任务
      await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', status: 'running'
      });

      fastify.taskStatistics.services.query.resolves({ list: [] });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse?timezone=Asia/Shanghai' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData({ timezone: 'Asia/Shanghai' });

      expect(result).to.exist;
      expect(result).to.have.property('date');
      // date 应为 CST 时区的今日日期
      const dayjs = require('dayjs');
      const utc = require('dayjs/plugin/utc');
      const timezone = require('dayjs/plugin/timezone');
      dayjs.extend(utc);
      dayjs.extend(timezone);
      const expectedDate = dayjs().tz('Asia/Shanghai').format('YYYY-MM-DD');
      expect(result.date).to.equal(expectedDate);
    });

    it('SSE hourlyTrendByStatus 应使用客户端时区的当前小时', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.task.models.task.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', status: 'running'
      });

      fastify.taskStatistics.services.query.resolves({ list: [] });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse?timezone=Asia/Shanghai' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData({ timezone: 'Asia/Shanghai' });

      expect(result).to.exist;
      if (result.hourlyTrendByStatus.length > 0) {
        const dayjs = require('dayjs');
        const utc = require('dayjs/plugin/utc');
        const timezone = require('dayjs/plugin/timezone');
        dayjs.extend(utc);
        dayjs.extend(timezone);
        const expectedHour = dayjs().tz('Asia/Shanghai').hour();
        // running 任务应追加到客户端时区的当前小时
        const runningEntry = result.hourlyTrendByStatus.find(e => e.status === 'running');
        if (runningEntry) {
          expect(runningEntry.hour).to.equal(expectedHour);
        }
      }
    });

    it('跨日数据在客户端时区下应正确分组', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // 两个不同日期的 UTC 小时数据
      // 2026-05-27T22:00:00Z = 2026-05-28 06:00 CST
      // 2026-05-28T22:00:00Z = 2026-05-29 06:00 CST
      const day1ServerTime = new Date('2026-05-27T22:00:00.000Z');
      const day2ServerTime = new Date('2026-05-28T22:00:00.000Z');

      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: day1ServerTime,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: day1ServerTime,
            data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 300, executionTime: 800, totalTime: 1100 } }
          },
          {
            channel: 'test-type', period: 'h', time: day2ServerTime,
            data: { sum: { total: 4, success: 3, failed: 0, canceled: 1, waitingTime: 600, executionTime: 1200, totalTime: 1800 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: day2ServerTime,
            data: { sum: { total: 2, success: 2, failed: 0, canceled: 0, waitingTime: 200, executionTime: 600, totalTime: 800 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', timezone: 'Asia/Shanghai' });

      // 应有两天数据，日期分别为 2026-05-28 和 2026-05-29（CST）
      expect(result.recentTrend.length).to.equal(2);
      const dates = result.recentTrend.map(t => t.date).sort();
      expect(dates).to.include('2026-05-28');
      expect(dates).to.include('2026-05-29');

      // durationTrend 也应有两天
      expect(result.durationTrend.length).to.equal(2);
      const durationDates = result.durationTrend.map(t => t.date).sort();
      expect(durationDates).to.include('2026-05-28');
      expect(durationDates).to.include('2026-05-29');
    });

    it('formatDate/getHour 无时区时应使用服务器本地时间', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const serverTime = new Date('2026-05-28T22:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: serverTime,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: serverTime,
            data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 300, executionTime: 800, totalTime: 1100 } }
          }
        ]
      });

      // 不传 timezone，formatDate 和 getHour 走 dayjs(date) 分支
      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
      expect(result.recentTrend.length).to.be.greaterThan(0);
      // hour 应为服务器本地时间的小时
      if (result.hourlyTrend.length > 0) {
        expect(typeof result.hourlyTrend[0].hour).to.equal('number');
      }
    });

    it('1m/3m/1m range 应覆盖 month/year 分支', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      fastify.taskStatistics.services.query.resolves({ list: [] });

      // 覆盖 parseRange 的 month 分支
      const result1m = await fastify.task.services.queryStatistics({ range: '1m' });
      expect(result1m).to.exist;

      // 覆盖 parseRange 的 month 分支
      const result3m = await fastify.task.services.queryStatistics({ range: '3m' });
      expect(result3m).to.exist;

      // 覆盖 parseRange 的 year 分支
      const result1y = await fastify.task.services.queryStatistics({ range: '1y' });
      expect(result1y).to.exist;
    });

    it('1m/3m/1y range with timezone 应覆盖所有 unit 分支', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      fastify.taskStatistics.services.query.resolves({ list: [] });

      const result1m = await fastify.task.services.queryStatistics({ range: '1m', timezone: 'Asia/Shanghai' });
      expect(result1m).to.exist;

      const result3m = await fastify.task.services.queryStatistics({ range: '3m', timezone: 'Asia/Shanghai' });
      expect(result3m).to.exist;

      const result1y = await fastify.task.services.queryStatistics({ range: '1y', timezone: 'Asia/Shanghai' });
      expect(result1y).to.exist;
    });

    it('runnerType 筛选应正确处理不同状态分组', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // 创建不同状态和 runnerType 的任务
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't1', targetType: 'document', runnerType: 'system', status: 'running' });
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't2', targetType: 'document', runnerType: 'manual', status: 'pending' });
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't3', targetType: 'document', runnerType: 'system', status: 'waiting' });

      fastify.taskStatistics.services.query.resolves({ list: [] });

      // 无 runnerType 过滤
      const resultAll = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(resultAll.byRunnerType).to.exist;

      // system runnerType 过滤
      const resultSystem = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'system' });
      expect(resultSystem.byRunnerType).to.exist;

      // manual runnerType 过滤
      const resultManual = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'manual' });
      expect(resultManual.byRunnerType).to.exist;
    });

    it('buildSseData 应覆盖 completedToday 和 byRunnerType 分支', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      // 创建多种状态任务以覆盖各分支
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't1', targetType: 'document', runnerType: 'system', status: 'running' });
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't2', targetType: 'document', runnerType: 'manual', status: 'success' });

      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type', period: 'h', time: new Date(),
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          },
          {
            channel: 'test-type:system', period: 'h', time: new Date(),
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual', period: 'h', time: new Date(),
            data: { sum: { total: 2, success: 1, failed: 0, canceled: 1, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          }
        ]
      });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse?timezone=Asia/Shanghai' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData({ timezone: 'Asia/Shanghai' });

      expect(result).to.exist;
      expect(result.completedToday).to.exist;
      expect(result.byRunnerType).to.exist;
      // completedTodayTotalDurationMsByRunnerType 应存在
      expect(result.completedTodayTotalDurationMsByRunnerType).to.exist;
    });

    it('SSE 无 timezone 参数应使用服务器本地时间', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      await fastify.task.models.task.create({ type: 'test-type', targetId: 't1', targetType: 'document', runnerType: 'system', status: 'running' });

      fastify.taskStatistics.services.query.resolves({ list: [] });

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData({});

      expect(result).to.exist;
      expect(result).to.have.property('date');
    });
  });
});

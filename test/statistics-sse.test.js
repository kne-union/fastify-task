const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - statistics sse data', function () {
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

  describe('SSE 统计接口测试', () => {
    it('should call sseStatistics with correct params', async () => {
      fastify = await createFastify();
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
      fastify = await createFastify();
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
      fastify = await createFastify();
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

  describe('buildSseData 测试', () => {
    it('should build SSE data with tasks in various states', async () => {
      fastify = await createFastify();
      await fastify.ready();

      // Create tasks in different states
      const pendingTask = await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        status: 'pending'
      });
      const runningTask = await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'system',
        status: 'running'
      });
      const waitingTask = await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-3',
        targetType: 'document',
        runnerType: 'manual',
        status: 'waiting'
      });
      const successTask = await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-4',
        targetType: 'document',
        runnerType: 'manual',
        status: 'success'
      });
      const failedTask = await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-5',
        targetType: 'document',
        runnerType: 'system',
        status: 'failed'
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
      fastify = await createFastify();
      await fastify.ready();

      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: now,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 2000, executionTime: 5000, totalTime: 7000 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: now,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 1000, executionTime: 3000, totalTime: 4000 } }
          },
          {
            channel: 'test-type:system',
            period: 'h',
            time: now,
            data: { sum: { total: 2, success: 1, failed: 0, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          }
        ]
      });

      // Create a running task
      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        status: 'running'
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
      fastify = await createFastify();
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
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        status: 'running'
      });
      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'manual',
        status: 'pending'
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
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        status: 'pending',
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
      fastify = await createFastify();
      await fastify.ready();

      // Make buildSseData throw by breaking the model
      fastify.task.models.task.findAll = async () => {
        throw new Error('DB connection lost');
      };

      await fastify.inject({ method: 'GET', url: '/api/task/statistics/sse' });
      const sendArgs = fastify.taskStatistics.services.sseStream.send.firstCall.args;
      const { fetchData } = sendArgs[1];
      const result = await fetchData();
      expect(result).to.deep.equal({});
    });
  });

  describe('buildSseData completedToday 时长数据测试', () => {
    it('should include completedTodayTotalDurationMsByRunnerType', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type:manual',
            period: 'h',
            time: now,
            data: { sum: { total: 2, success: 2, failed: 0, canceled: 0, waitingTime: 1000, executionTime: 3000, totalTime: 4000 } }
          },
          {
            channel: 'test-type:system',
            period: 'h',
            time: now,
            data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 500, executionTime: 2000, totalTime: 2500 } }
          }
        ]
      });
      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 't1',
        targetType: 'doc',
        runnerType: 'manual',
        status: 'running'
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
});

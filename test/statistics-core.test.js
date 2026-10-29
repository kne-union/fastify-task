const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext, mockStatQuery } = require('./helpers/context');

describe('@kne/fastify-task - statistics core queries', function () {
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
      expect(callArgs.channel).to.match(/^test-type:manual:\d+$/);
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
      expect(callArgs.channel).to.match(/^test-type:system:\d+$/);
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

    it('should skip collect when task is not in a terminal status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const getTaskServiceContext = require('../libs/helpers/task-service-context');
      const taskContext = getTaskServiceContext(fastify, fastify.task.options);
      await taskContext.collectTaskStatistics({ type: 'test-type', runnerType: 'system', status: 'pending', createdAt: new Date() });

      expect(fastify.taskStatistics.services.collect.called).to.be.false;
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
      expect(body).to.have.property('totalTasks');
      expect(body).to.have.property('byStatus');
      expect(body).to.have.property('byType');
    });

    it('should return durationTrend in statistics response', async () => {
      fastify = await createFastify();
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
      fastify = await createFastify();
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
      fastify = await createFastify();
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

    it('should split 1m range into 30 daily buckets in client timezone', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const clock = sinon.useFakeTimers(new Date('2026-06-09T09:27:30.000Z'));
      try {
        const result = await fastify.task.services.queryStatistics({ range: '1m', timezone: 'Asia/Shanghai' });

        expect(result.granularity).to.equal('day');
        expect(result.buckets).to.have.length(30);
        expect(result.buckets[0].key).to.equal('2026-05-11');
        expect(result.buckets[29].key).to.equal('2026-06-09');
        expect(result.recentTrend.map(t => t.date)).to.deep.equal(result.buckets.map(b => b.key));

        const mainCalls = fastify.taskStatistics.services.query.getCalls().filter(c => c.args[0].channels.includes('test-type'));
        expect(mainCalls).to.have.length(30);
        const firstArgs = mainCalls.map(c => c.args[0]).sort((a, b) => a.startTime - b.startTime)[0];
        expect(firstArgs.startTime.toISOString()).to.equal('2026-05-10T16:00:00.000Z');
        expect(firstArgs.endTime.toISOString()).to.equal('2026-05-11T16:00:00.000Z');
        expect(result.endTime.toISOString()).to.equal('2026-06-09T09:27:30.000Z');
      } finally {
        clock.restore();
      }
    });

    it('should split 3m range into 13 weekly buckets starting on Monday', async () => {
      fastify = await createFastify();
      await fastify.ready();

      // 2026-06-10 为周三（Asia/Shanghai）
      const clock = sinon.useFakeTimers(new Date('2026-06-10T04:00:00.000Z'));
      try {
        const result = await fastify.task.services.queryStatistics({ range: '3m', timezone: 'Asia/Shanghai' });

        expect(result.granularity).to.equal('week');
        expect(result.buckets).to.have.length(13);
        expect(result.buckets[12].key).to.equal('2026-06-08');
        expect(result.buckets[0].key).to.equal('2026-03-16');
      } finally {
        clock.restore();
      }
    });

    it('should split 1y range into 12 monthly buckets and sum each month', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const clock = sinon.useFakeTimers(new Date('2026-06-10T04:00:00.000Z'));
      try {
        mockStatQuery(fastify.taskStatistics.services.query, [
          { channel: 'test-type', period: 'm', time: new Date('2026-01-01T00:00:00.000+08:00'), data: { sum: { total: 9, success: 9 } } },
          { channel: 'test-type', period: 'm', time: new Date('2026-04-01T00:00:00.000+08:00'), data: { sum: { total: 4, success: 3, failed: 1 } } },
          { channel: 'test-type', period: 'd', time: new Date('2026-06-02T00:00:00.000+08:00'), data: { sum: { total: 2, canceled: 2 } } }
        ]);
        const result = await fastify.task.services.queryStatistics({ range: '1y', timezone: 'Asia/Shanghai' });

        expect(result.granularity).to.equal('month');
        expect(result.buckets.map(b => b.key)).to.deep.equal(['2025-07', '2025-08', '2025-09', '2025-10', '2025-11', '2025-12', '2026-01', '2026-02', '2026-03', '2026-04', '2026-05', '2026-06']);
        const countOf = key => result.recentTrend.find(t => t.date === key).count;
        expect(countOf('2026-01')).to.equal(9);
        expect(countOf('2026-04')).to.equal(4);
        expect(countOf('2026-06')).to.equal(2);
        expect(countOf('2025-07')).to.equal(0);
        expect(result.recentTrendByStatus).to.deep.include({ date: '2026-04', status: 'failed', count: 1 });
        expect(result.totalTasks).to.equal(15);
      } finally {
        clock.restore();
      }
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

  describe('queryAndParse 数据处理测试', () => {
    const buildMockQueryResult = () => {
      const now = new Date();
      return {
        list: [
          // 1-segment channel, daily
          {
            channel: 'test-type',
            period: 'd',
            time: now,
            data: { sum: { total: 10, success: 8, failed: 1, canceled: 1, waitingTime: 5000, executionTime: 10000, totalTime: 15000 } }
          },
          // 2-segment channel, daily
          {
            channel: 'test-type:manual',
            period: 'd',
            time: now,
            data: { sum: { total: 5, success: 4, failed: 1, canceled: 0, waitingTime: 2000, executionTime: 5000, totalTime: 7000 } }
          },
          // 2-segment channel system, daily
          {
            channel: 'test-type:system',
            period: 'd',
            time: now,
            data: { sum: { total: 5, success: 4, failed: 0, canceled: 1, waitingTime: 3000, executionTime: 5000, totalTime: 8000 } }
          },
          // 1-segment channel, hourly
          {
            channel: 'test-type',
            period: 'h',
            time: now,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 1000, executionTime: 3000, totalTime: 4000 } }
          },
          // 2-segment channel, hourly
          {
            channel: 'test-type:manual',
            period: 'h',
            time: now,
            data: { sum: { total: 2, success: 2, failed: 0, canceled: 0, waitingTime: 500, executionTime: 2000, totalTime: 2500 } }
          },
          // 2-segment channel system, hourly
          {
            channel: 'test-type:system',
            period: 'h',
            time: now,
            data: { sum: { total: 1, success: 0, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          }
        ]
      };
    };

    it('should process statistics data with 1-segment and 2-segment channels', async () => {
      fastify = await createFastify();
      await fastify.ready();
      mockStatQuery(fastify.taskStatistics.services.query, buildMockQueryResult().list);

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
      // 同一 runnerType + type 的多个窗口需累加，而非后者覆盖前者
      expect(result.duration.byTypeByRunnerType.manual['test-type'].count).to.equal(7);
      expect(result.duration.byTypeByRunnerType.system['test-type'].count).to.equal(6);
      expect(result.duration.canceledCount).to.equal(1);
    });

    it('should build daily trend arrays from statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();
      mockStatQuery(fastify.taskStatistics.services.query, buildMockQueryResult().list);

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.recentTrend).to.exist;
      expect(Array.isArray(result.recentTrend)).to.be.true;
      expect(result.recentTrendByStatus).to.exist;
      expect(result.recentTrendByType).to.exist;
    });

    it('should build hourly trend arrays from statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();
      mockStatQuery(fastify.taskStatistics.services.query, buildMockQueryResult().list);

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.hourlyTrend).to.exist;
      expect(Array.isArray(result.hourlyTrend)).to.be.true;
      expect(result.hourlyCompletionTrend).to.exist;
      expect(Array.isArray(result.hourlyCompletionTrend)).to.be.true;
    });

    it('should build hourly trend from aggregated child hour channels', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const day = new Date('2026-05-26T00:00:00.000Z');
      mockStatQuery(fastify.taskStatistics.services.query, [
        {
          channel: 'test-type',
          period: 'd',
          time: day,
          data: { sum: { total: 3, success: 2, failed: 1, canceled: 0 } }
        },
        {
          channel: 'test-type:manual',
          period: 'd',
          time: day,
          data: { sum: { total: 3, success: 2, failed: 1, canceled: 0 } }
        },
        {
          channel: 'test-type:manual:9',
          period: 'd',
          time: day,
          data: { sum: { total: 3, success: 2, failed: 1, canceled: 0 } }
        }
      ]);

      const clock = sinon.useFakeTimers(new Date('2026-05-28T04:00:00.000Z'));
      let result;
      try {
        result = await fastify.task.services.queryStatistics({ range: '1m' });
      } finally {
        clock.restore();
      }

      const hourlyCall = fastify.taskStatistics.services.query.getCalls().find(c => c.args[0].channels.includes('test-type:manual:9'));
      expect(hourlyCall).to.exist;
      expect(hourlyCall.args[0].includeChildren).to.be.undefined;
      expect(result.hourlyTrend).to.deep.include({
        date: '2026-05-26',
        hour: 9,
        total: 3,
        success: 2,
        failed: 1,
        canceled: 0
      });
      expect(result.hourlyCompletionTrend).to.deep.include({
        date: '2026-05-26',
        hour: 9,
        type: 'test-type',
        runnerType: 'manual',
        totalCompleted: 3,
        successCount: 2,
        failedCount: 1,
        canceledCount: 0
      });
    });

    it('should build duration trend from statistics', async () => {
      fastify = await createFastify();
      await fastify.ready();
      mockStatQuery(fastify.taskStatistics.services.query, buildMockQueryResult().list);

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
        expect(trendItem).to.have.property('byTypeByRunnerType');
        expect(trendItem.byTypeByRunnerType.manual['test-type'].count).to.equal(7);
        expect(trendItem.byTypeByRunnerType.system['test-type'].count).to.equal(6);
      }
    });

    it('should filter by runnerType in queryAndParse', async () => {
      fastify = await createFastify();
      await fastify.ready();
      mockStatQuery(fastify.taskStatistics.services.query, buildMockQueryResult().list);

      const result = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'manual' });
      // runnerType filter should exclude items that don't match
      expect(result).to.exist;
    });

    it('should return empty result when channels are empty', async () => {
      fastify = await createFastify();
      await fastify.ready();
      fastify.taskStatistics.services.channelMeta.list.resolves([]);
      fastify.taskStatistics.services.query.resolves({ list: [] });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.totalTasks).to.equal(0);
    });

    it('should handle queryStatistics error gracefully', async () => {
      fastify = await createFastify();
      await fastify.ready();
      fastify.taskStatistics.services.query.rejects(new Error('DB error'));

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
      expect(result.totalTasks).to.equal(0);
    });
  });

  describe('queryAndParse 边界分支测试', () => {
    it('should handle item with NaN values in sum', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'd',
            time: now,
            data: { sum: { total: 'invalid', success: NaN, failed: undefined, canceled: null, waitingTime: 'bad', executionTime: NaN, totalTime: null } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result).to.exist;
      // NaN/invalid values should be converted to 0 by safeNum
      expect(result.byStatus.success).to.equal(0);
    });

    it('should handle channel with only one segment (no runnerType)', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.channelMeta.list.resolves([{ channel: 'simple-type' }]);
      mockStatQuery(fastify.taskStatistics.services.query, [
        {
          channel: 'simple-type',
          period: 'd',
          time: now,
          data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 100, executionTime: 200, totalTime: 300 } }
        }
      ]);

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.totalTasks).to.equal(1);
      expect(result.byType['simple-type']).to.equal(1);
    });

    it('should count only terminal statuses while averaging duration by total', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.channelMeta.list.resolves([{ channel: 'test-type' }, { channel: 'test-type:system' }]);
      // total 比终态多 2 条：旧版本失败重试时采集的 pending 记录
      const sum = { total: 6, success: 3, failed: 1, pending: 2, waitingTime: 600, executionTime: 1200, totalTime: 1800 };
      mockStatQuery(fastify.taskStatistics.services.query, [
        { channel: 'test-type', period: 'd', time: now, data: { sum } },
        { channel: 'test-type:system', period: 'd', time: now, data: { sum } }
      ]);

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.totalTasks).to.equal(4);
      expect(result.byType['test-type']).to.equal(4);
      expect(result.byRunnerType.system).to.equal(4);
      expect(result.recentTrend.reduce((n, item) => n + item.count, 0)).to.equal(4);
      expect(result.duration.completedCount).to.equal(4);
      expect(result.duration.avgExecutionTime).to.equal(200);
      expect(result.duration.byRunnerType.system.avgExecutionTime).to.equal(200);
    });

    it('should handle runnerType filter excluding items without runnerTypeName', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'd',
            time: now,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1 } }
          },
          {
            channel: 'test-type:manual',
            period: 'd',
            time: now,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'manual' });
      // With runnerType filter, 1-segment channels without runnerTypeName should be excluded
      expect(result).to.exist;
    });
  });

  describe('queryAndParse 多日/多时排序测试', () => {
    it('should sort durationTrend by date when multiple dates exist', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      const yesterday = new Date(now.getTime() - 86400000);
      mockStatQuery(fastify.taskStatistics.services.query, [
        {
          channel: 'test-type',
          period: 'd',
          time: yesterday,
          data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
        },
        {
          channel: 'test-type',
          period: 'd',
          time: now,
          data: { sum: { total: 5, success: 4, failed: 0, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
        }
      ]);

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.durationTrend.length).to.equal(2);
      // Should be sorted by date ascending
      expect(result.durationTrend[0].date < result.durationTrend[1].date).to.be.true;
    });

    it('should sort hourly allHours when multiple hours exist', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const now = new Date();
      const hourAgo = new Date(now.getTime() - 3600000);
      mockStatQuery(fastify.taskStatistics.services.query, [
        {
          channel: 'test-type',
          period: 'd',
          time: hourAgo,
          data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
        },
        {
          channel: 'test-type',
          period: 'd',
          time: now,
          data: { sum: { total: 3, success: 2, failed: 0, canceled: 1, waitingTime: 800, executionTime: 1500, totalTime: 2300 } }
        },
        {
          channel: 'test-type:manual:8',
          period: 'd',
          time: hourAgo,
          data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 300, executionTime: 800, totalTime: 1100 } }
        },
        {
          channel: 'test-type:manual:9',
          period: 'd',
          time: hourAgo,
          data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 300, executionTime: 800, totalTime: 1100 } }
        },
        {
          channel: 'test-type:system:10',
          period: 'd',
          time: now,
          data: { sum: { total: 1, success: 0, failed: 1, canceled: 0, waitingTime: 200, executionTime: 600, totalTime: 800 } }
        }
      ]);

      const result = await fastify.task.services.queryStatistics({ range: '7d' });
      expect(result.hourlyTrend.length).to.be.greaterThan(0);
      expect(result.hourlyCompletionTrend.length).to.be.greaterThan(0);
    });
  });
});

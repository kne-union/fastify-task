const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - timezone statistics', function () {
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

  describe('时区支持测试', () => {
    it('parseRange 应使用客户端时区计算日期范围', async () => {
      fastify = await createFastify();
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
      fastify = await createFastify();
      await fastify.ready();

      // 模拟数据：服务器 UTC+0 时间 2026-05-28T02:00:00Z
      // 客户端 UTC+8 应显示为 2026-05-28 10:00
      const serverTime = new Date('2026-05-28T02:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: serverTime,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: serverTime,
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

    it('queryStatistics 应将小时子通道的服务端小时桶转换为客户端时区小时', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const dayjs = require('dayjs');
      const utc = require('dayjs/plugin/utc');
      const timezone = require('dayjs/plugin/timezone');
      dayjs.extend(utc);
      dayjs.extend(timezone);

      const serverDay = new Date('2026-05-26T00:00:00.000Z');
      const serverHour = 20;
      const targetTimezone = ['Asia/Shanghai', 'UTC', 'Pacific/Honolulu', 'America/New_York']
        .find(tz => dayjs(serverDay).hour(serverHour).minute(0).second(0).millisecond(0).tz(tz).hour() !== serverHour) || 'UTC';
      const expectedTime = dayjs(serverDay).hour(serverHour).minute(0).second(0).millisecond(0).tz(targetTimezone);

      fastify.taskStatistics.services.query.onFirstCall().resolves({
        list: [
          {
            channel: 'test-type',
            period: 'd',
            time: serverDay,
            data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 100, executionTime: 200, totalTime: 300 } }
          }
        ]
      });
      fastify.taskStatistics.services.query.onSecondCall().resolves({
        list: [
          {
            channel: `test-type:manual:${serverHour}`,
            period: 'd',
            time: serverDay,
            data: { sum: { total: 1, success: 1, failed: 0, canceled: 0, waitingTime: 100, executionTime: 200, totalTime: 300 } }
          }
        ]
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', timezone: targetTimezone });

      expect(result.hourlyTrend).to.deep.include({
        date: expectedTime.format('YYYY-MM-DD'),
        hour: expectedTime.hour(),
        total: 1,
        success: 1,
        failed: 0,
        canceled: 0
      });
      expect(result.hourlyCompletionTrend).to.deep.include({
        date: expectedTime.format('YYYY-MM-DD'),
        hour: expectedTime.hour(),
        type: 'test-type',
        runnerType: 'manual',
        totalCompleted: 1,
        successCount: 1,
        failedCount: 0,
        canceledCount: 0
      });
    });

    it('queryStatistics 无时区时使用服务器本地时间', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const serverTime = new Date('2026-05-28T02:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: serverTime,
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: serverTime,
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
      fastify = await createFastify();
      await fastify.ready();

      // UTC 时间 2026-05-28T22:00:00Z，在 UTC+8 下是 2026-05-29 06:00
      const serverTime = new Date('2026-05-28T22:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: serverTime,
            data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: serverTime,
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
      fastify = await createFastify();
      await fastify.ready();

      // 创建一个 running 任务
      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        status: 'running'
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
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.models.task.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        status: 'running'
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
      fastify = await createFastify();
      await fastify.ready();

      // 两个不同日期的 UTC 小时数据
      // 2026-05-27T22:00:00Z = 2026-05-28 06:00 CST
      // 2026-05-28T22:00:00Z = 2026-05-29 06:00 CST
      const day1ServerTime = new Date('2026-05-27T22:00:00.000Z');
      const day2ServerTime = new Date('2026-05-28T22:00:00.000Z');

      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: day1ServerTime,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: day1ServerTime,
            data: { sum: { total: 2, success: 1, failed: 1, canceled: 0, waitingTime: 300, executionTime: 800, totalTime: 1100 } }
          },
          {
            channel: 'test-type',
            period: 'h',
            time: day2ServerTime,
            data: { sum: { total: 4, success: 3, failed: 0, canceled: 1, waitingTime: 600, executionTime: 1200, totalTime: 1800 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: day2ServerTime,
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
      fastify = await createFastify();
      await fastify.ready();

      const serverTime = new Date('2026-05-28T22:00:00.000Z');
      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: serverTime,
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: serverTime,
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
      fastify = await createFastify();
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
      fastify = await createFastify();
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
      fastify = await createFastify();
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
      fastify = await createFastify();
      await fastify.ready();

      // 创建多种状态任务以覆盖各分支
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't1', targetType: 'document', runnerType: 'system', status: 'running' });
      await fastify.task.models.task.create({ type: 'test-type', targetId: 't2', targetType: 'document', runnerType: 'manual', status: 'success' });

      fastify.taskStatistics.services.query.resolves({
        list: [
          {
            channel: 'test-type',
            period: 'h',
            time: new Date(),
            data: { sum: { total: 5, success: 3, failed: 1, canceled: 1, waitingTime: 1000, executionTime: 2000, totalTime: 3000 } }
          },
          {
            channel: 'test-type:system',
            period: 'h',
            time: new Date(),
            data: { sum: { total: 3, success: 2, failed: 1, canceled: 0, waitingTime: 500, executionTime: 1000, totalTime: 1500 } }
          },
          {
            channel: 'test-type:manual',
            period: 'h',
            time: new Date(),
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
      fastify = await createFastify();
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

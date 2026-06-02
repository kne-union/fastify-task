const fp = require('fastify-plugin');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const {
  CONFIG_CONSTANTS,
  getConfigManager
} = require('../utils/config');

dayjs.extend(utc);
dayjs.extend(timezone);

// 查询用客户端时间：parseRange 使用客户端时区计算日期范围
const parseRange = (range = '7d', tz) => {
  const config = CONFIG_CONSTANTS.TIME_RANGES[range];
  if (!config) {
    throw new Error(`不支持的时间范围: ${range}, 支持: ${Object.keys(CONFIG_CONSTANTS.TIME_RANGES).join(',')}`);
  }
  // 写入数据全使用服务器时间，查询用客户端时间
  // 使用客户端时区计算"现在"和"起始时间"，确保日期边界对齐客户端视角
  const now = tz ? dayjs().tz(tz) : dayjs();
  const unitStartMap = { day: 'day', month: 'month', year: 'year' };
  const startUnit = unitStartMap[config.unit];
  const startTime = startUnit
    ? now.subtract(config.value, startUnit).startOf(startUnit)
    : now.subtract(config.value, config.unit);
  return { startTime: startTime.toDate(), endTime: now.toDate(), label: config.label, range };
};

// 查询用客户端时间：formatDate 使用客户端时区格式化日期
const formatDate = (date, tz) => {
  const d = tz ? dayjs(date).tz(tz) : dayjs(date);
  return d.format('YYYY-MM-DD');
};

// 查询用客户端时间：提取客户端时区下的小时
const getHour = (date, tz) => {
  const d = tz ? dayjs(date).tz(tz) : dayjs(date);
  return d.hour();
};

const safeNum = v => {
  const n = Number(v);
  return (typeof n === 'number' && !isNaN(n) ? n : 0);
};

const getSumData = item => (item.data || {}).sum || item.data || {};

const computeAvg = (sum, count) => (count > 0 ? Math.round(sum / count) : 0);

// 将原始时长累加器 {count, sum*} 转为输出格式 {count, avg*}
const toAvgDuration = ({ count, sumWaiting, sumExecution, sumTotal }) => ({
  count,
  avgWaitingTime: computeAvg(sumWaiting, count),
  avgExecutionTime: computeAvg(sumExecution, count),
  avgTotalTime: computeAvg(sumTotal, count)
});

const parseChannel = channel => {
  const parts = channel.split(':');
  return parts.length >= 2
    ? { type: parts[0], runnerType: parts[1] }
    : { type: channel, runnerType: null };
};

const buildChannels = async (statisticsServices, type, runnerType) => {
  const metaResult = await statisticsServices.channelMeta.list();
  const list = Array.isArray(metaResult) ? metaResult : metaResult.list || [];
  const rootChannels = list.map(meta => meta.channel).filter(Boolean);
  const types = type ? rootChannels.filter(ch => ch === type) : rootChannels;
  const rts = runnerType ? [runnerType] : [CONFIG_CONSTANTS.RUNNER_TYPES.SYSTEM, CONFIG_CONSTANTS.RUNNER_TYPES.MANUAL];
  const channels = [];
  for (const t of types) {
    channels.push(t);
    for (const rt of rts) channels.push(`${t}:${rt}`);
  }
  return channels;
};

// 确保时长累加器存在于 map 中
const ensureDurationAcc = (map, key) => {
  if (!map[key]) map[key] = { count: 0, sumWaiting: 0, sumExecution: 0, sumTotal: 0 };
  return map[key];
};

// 累加时长到累加器
const accumDuration = (acc, count, sum) => {
  acc.count += count;
  acc.sumWaiting += safeNum(sum.waitingTime);
  acc.sumExecution += safeNum(sum.executionTime);
  acc.sumTotal += safeNum(sum.totalTime);
};

// 创建空的 dailyMap 条目
const createDailyMapEntry = () => ({
  total: 0,
  byStatus: {
    [CONFIG_CONSTANTS.TASK_STATUSES.SUCCESS]: 0,
    [CONFIG_CONSTANTS.TASK_STATUSES.FAILED]: 0,
    [CONFIG_CONSTANTS.TASK_STATUSES.CANCELED]: 0
  },
  byType: {}
});

// 创建空的 dailyDurationMap 条目
const createDailyDurationEntry = () => ({
  completedCount: 0,
  successCount: 0,
  failedCount: 0,
  canceledCount: 0,
  sumWaiting: 0,
  sumExecution: 0,
  sumTotal: 0,
  countWithTiming: 0,
  byType: {},
  byRunnerType: {}
});

// 查询 statistics 并解析结果
// 写入数据全使用服务器时间（DB 中存储的是服务器时间），查询用客户端时间转换展示
const queryAndParse = async (statisticsServices, { channels, startTime, endTime, timezone: tz, runnerType: runnerTypeFilter }) => {
  if (channels.length === 0) return null;

  const statResult = await statisticsServices.query({
    channels, startTime, endTime,
    attributeNames: CONFIG_CONSTANTS.STATISTICS_ATTRIBUTES,
    aggregates: ['sum'],
    timezone: tz || undefined
  });

  const result = {
    totalTasks: 0,
    byStatus: { success: 0, failed: 0, canceled: 0 },
    byType: {},
    byRunnerType: {},
    dailyMap: {},
    hourlyMap: {},
    hourlyStatusMap: {},
    hourlyTypeMap: {},
    hourlyDetailMap: {},
    duration: {
      completedCount: 0, successCount: 0, failedCount: 0, canceledCount: 0,
      sumWaiting: 0, sumExecution: 0, sumTotal: 0, countWithTiming: 0,
      byType: {}, byRunnerType: {}, byTypeByRunnerType: {}
    },
    durationTrend: []
  };
  const dailyDurationMap = {};

  for (const item of statResult.list || []) {
    const { type: typeName, runnerType: runnerTypeName } = parseChannel(item.channel);
    if (runnerTypeFilter && runnerTypeName && runnerTypeName !== runnerTypeFilter) continue;
    if (runnerTypeFilter && !runnerTypeName) continue;

    const sum = getSumData(item);
    const count = safeNum(sum.total);
    const isHourly = item.period === 'h';
    // 查询用客户端时间：日期和小时使用客户端时区转换
    const date = formatDate(item.time, tz);
    const hour = isHourly ? getHour(item.time, tz) : null;

    // ─── 1-segment channel: 全局 + 按日 + 时长 聚合 ───
    if (!runnerTypeName) {
      // 全局汇总
      result.totalTasks += count;
      result.byStatus.success += safeNum(sum.success);
      result.byStatus.failed += safeNum(sum.failed);
      result.byStatus.canceled += safeNum(sum.canceled);
      result.byType[typeName] = (result.byType[typeName] || 0) + count;

      // 全局时长
      result.duration.completedCount += count;
      result.duration.successCount += safeNum(sum.success);
      result.duration.failedCount += safeNum(sum.failed);
      result.duration.canceled += safeNum(sum.canceled);
      result.duration.sumWaiting += safeNum(sum.waitingTime);
      result.duration.sumExecution += safeNum(sum.executionTime);
      result.duration.sumTotal += safeNum(sum.totalTime);
      result.duration.countWithTiming += count;
      if (count > 0) accumDuration(ensureDurationAcc(result.duration.byType, typeName), count, sum);

      // 按日汇总
      if (!result.dailyMap[date]) result.dailyMap[date] = createDailyMapEntry();
      const dm = result.dailyMap[date];
      dm.total += count;
      dm.byStatus.success += safeNum(sum.success);
      dm.byStatus.failed += safeNum(sum.failed);
      dm.byStatus.canceled += safeNum(sum.canceled);
      dm.byType[typeName] = (dm.byType[typeName] || 0) + count;

      // 按日时长
      if (!dailyDurationMap[date]) dailyDurationMap[date] = createDailyDurationEntry();
      const dd = dailyDurationMap[date];
      dd.completedCount += count;
      dd.successCount += safeNum(sum.success);
      dd.failedCount += safeNum(sum.failed);
      dd.canceledCount += safeNum(sum.canceled);
      dd.sumWaiting += safeNum(sum.waitingTime);
      dd.sumExecution += safeNum(sum.executionTime);
      dd.sumTotal += safeNum(sum.totalTime);
      dd.countWithTiming += count;
      if (count > 0) accumDuration(ensureDurationAcc(dd.byType, typeName), count, sum);

      // 小时级趋势（仅 period='h'）
      if (isHourly) {
        if (!result.hourlyMap[date]) result.hourlyMap[date] = {};
        result.hourlyMap[date][hour] = (result.hourlyMap[date][hour] || 0) + count;

        if (!result.hourlyStatusMap[date]) result.hourlyStatusMap[date] = {};
        if (!result.hourlyStatusMap[date][hour]) result.hourlyStatusMap[date][hour] = {};
        for (const status of ['success', 'failed', 'canceled']) {
          const sc = safeNum(sum[status]);
          if (sc > 0) result.hourlyStatusMap[date][hour][status] = (result.hourlyStatusMap[date][hour][status] || 0) + sc;
        }

        if (!result.hourlyTypeMap[date]) result.hourlyTypeMap[date] = {};
        if (!result.hourlyTypeMap[date][hour]) result.hourlyTypeMap[date][hour] = {};
        if (count > 0) result.hourlyTypeMap[date][hour][typeName] = (result.hourlyTypeMap[date][hour][typeName] || 0) + count;
      }
    }

    // ─── 2-segment channel: byRunnerType 聚合 ───
    if (runnerTypeName) {
      result.byRunnerType[runnerTypeName] = (result.byRunnerType[runnerTypeName] || 0) + count;

      if (count > 0) {
        accumDuration(ensureDurationAcc(result.duration.byRunnerType, runnerTypeName), count, sum);

        // byTypeByRunnerType（覆盖写入，非累加）
        if (!result.duration.byTypeByRunnerType[runnerTypeName]) result.duration.byTypeByRunnerType[runnerTypeName] = {};
        result.duration.byTypeByRunnerType[runnerTypeName][typeName] = {
          count,
          sumWaiting: safeNum(sum.waitingTime),
          sumExecution: safeNum(sum.executionTime),
          sumTotal: safeNum(sum.totalTime)
        };

        // 按日 byRunnerType 时长
        if (dailyDurationMap[date]) {
          accumDuration(ensureDurationAcc(dailyDurationMap[date].byRunnerType, runnerTypeName), count, sum);
        }
      }

      // hourlyDetailMap（仅 period='h'）
      if (isHourly) {
        if (!result.hourlyDetailMap[date]) result.hourlyDetailMap[date] = {};
        if (!result.hourlyDetailMap[date][hour]) result.hourlyDetailMap[date][hour] = {};
        if (!result.hourlyDetailMap[date][hour][typeName]) result.hourlyDetailMap[date][hour][typeName] = {};
        if (!result.hourlyDetailMap[date][hour][typeName][runnerTypeName]) {
          result.hourlyDetailMap[date][hour][typeName][runnerTypeName] = { total: 0, success: 0, failed: 0, canceled: 0 };
        }
        const detail = result.hourlyDetailMap[date][hour][typeName][runnerTypeName];
        detail.total += count;
        detail.success += safeNum(sum.success);
        detail.failed += safeNum(sum.failed);
        detail.canceled += safeNum(sum.canceled);
      }
    }
  }

  // 构建按日时长趋势
  result.durationTrend = Object.entries(dailyDurationMap)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, dd]) => ({
      date,
      completedCount: dd.completedCount,
      successCount: dd.successCount,
      failedCount: dd.failedCount,
      canceledCount: dd.canceledCount,
      avgWaitingTime: computeAvg(dd.sumWaiting, dd.countWithTiming),
      avgExecutionTime: computeAvg(dd.sumExecution, dd.countWithTiming),
      avgTotalTime: computeAvg(dd.sumTotal, dd.countWithTiming),
      byType: Object.fromEntries(Object.entries(dd.byType).map(([n, td]) => [n, toAvgDuration(td)])),
      byRunnerType: Object.fromEntries(Object.entries(dd.byRunnerType).map(([n, rt]) => [n, toAvgDuration(rt)]))
    }));

  return result;
};

// 构建 todayDuration 结果
const buildDurationResult = (d) => ({
  completedCount: d.completedCount,
  successCount: d.successCount,
  failedCount: d.failedCount,
  canceledCount: d.canceledCount,
  avgWaitingTime: computeAvg(d.sumWaiting, d.countWithTiming),
  avgExecutionTime: computeAvg(d.sumExecution, d.countWithTiming),
  avgTotalTime: computeAvg(d.sumTotal, d.countWithTiming),
  byType: Object.fromEntries(Object.entries(d.byType).map(([n, td]) => [n, toAvgDuration(td)])),
  byRunnerType: Object.fromEntries(Object.entries(d.byRunnerType).map(([n, rt]) => [n, toAvgDuration(rt)])),
  byTypeByRunnerType: Object.fromEntries(
    Object.entries(d.byTypeByRunnerType).map(([rtName, types]) => [
      rtName,
      Object.fromEntries(Object.entries(types).map(([typeName, td]) => [typeName, toAvgDuration(td)]))
    ])
  )
});

// 构建小时趋势数组（完整格式，供 history 接口使用）
const buildHourlyTrendArrays = (parsed) => {
  const hourlyTrend = [];
  const hourlyTrendByStatus = [];
  const hourlyTrendByType = [];
  const hourlyCompletionTrend = [];

  const dates = Object.keys(parsed.hourlyMap).sort();
  for (const date of dates) {
    const hoursMap = parsed.hourlyMap[date] || {};
    const statusMap = parsed.hourlyStatusMap[date] || {};
    const typeMap = parsed.hourlyTypeMap[date] || {};
    const detailMap = parsed.hourlyDetailMap[date] || {};

    const allHours = [...new Set([
      ...Object.keys(hoursMap).map(Number),
      ...Object.keys(statusMap).map(Number),
      ...Object.keys(typeMap).map(Number),
      ...Object.keys(detailMap).map(Number)
    ])].sort((a, b) => a - b);

    for (const hour of allHours) {
      const statuses = statusMap[hour] || {};
      hourlyTrend.push({
        date, hour,
        total: hoursMap[hour] || 0,
        success: statuses.success || 0,
        failed: statuses.failed || 0,
        canceled: statuses.canceled || 0
      });
      for (const [status, count] of Object.entries(statuses)) {
        if (count > 0) hourlyTrendByStatus.push({ date, hour, status, count });
      }
      const types = typeMap[hour] || {};
      for (const [typeName, count] of Object.entries(types)) {
        if (count > 0) hourlyTrendByType.push({ date, hour, type: typeName, count });
      }
    }

    for (const hour of Object.keys(detailMap).map(Number).sort((a, b) => a - b)) {
      const types = detailMap[hour] || {};
      for (const [typeName, runnerTypes] of Object.entries(types)) {
        for (const [runnerTypeName, detail] of Object.entries(runnerTypes)) {
          if (detail.total > 0) {
            hourlyCompletionTrend.push({
              date, hour, type: typeName, runnerType: runnerTypeName,
              totalCompleted: detail.total,
              successCount: detail.success,
              failedCount: detail.failed,
              canceledCount: detail.canceled
            });
          }
        }
      }
    }
  }

  return { hourlyTrend, hourlyTrendByStatus, hourlyTrendByType, hourlyCompletionTrend };
};

// 构建历史日趋势数组
const buildDailyTrendArrays = (parsed) => {
  const sorted = Object.entries(parsed.dailyMap).sort(([a], [b]) => a.localeCompare(b));
  const recentTrend = sorted.map(([date, d]) => ({ date, count: d.total }));
  const recentTrendByStatus = [];
  const recentTrendByType = [];
  sorted.forEach(([date, d]) => {
    Object.entries(d.byStatus).forEach(([status, count]) => recentTrendByStatus.push({ date, status, count }));
    Object.entries(d.byType).forEach(([type, count]) => recentTrendByType.push({ date, type, count }));
  });
  return { recentTrend, recentTrendByStatus, recentTrendByType };
};

module.exports = fp(async (fastify, options) => {
  const statisticsServices = fastify[`${options.name}Statistics`].services;
  const { task: TaskModel } = fastify[options.name].models;
  const { Op, fn, col } = fastify.sequelize.Sequelize;

  // ─── queryStatistics (history 接口) ───
  const queryStatistics = async ({ range = '7d', timezone, type, runnerType }) => {
    // 查询用客户端时间：parseRange 传入 timezone
    const { startTime, endTime, label, range: rangeKey } = parseRange(range, timezone);
    const emptyResult = {
      range: rangeKey, rangeLabel: label,
      totalTasks: 0, byStatus: {}, byType: {}, byRunnerType: {},
      recentTrend: [], recentTrendByStatus: [], recentTrendByType: [],
      durationTrend: [], hourlyCompletionTrend: []
    };
    try {
      const channels = await buildChannels(statisticsServices, type, runnerType);
      const parsed = await queryAndParse(statisticsServices, { channels, startTime, endTime, timezone, runnerType });
      if (!parsed) return emptyResult;

      const { recentTrend, recentTrendByStatus, recentTrendByType } = buildDailyTrendArrays(parsed);
      const { hourlyTrend, hourlyCompletionTrend } = buildHourlyTrendArrays(parsed);

      return {
        range: rangeKey, rangeLabel: label,
        totalTasks: parsed.totalTasks,
        byStatus: parsed.byStatus,
        byType: parsed.byType,
        byRunnerType: parsed.byRunnerType,
        recentTrend, recentTrendByStatus, recentTrendByType,
        durationTrend: parsed.durationTrend,
        hourlyTrend, hourlyCompletionTrend
      };
    } catch (e) {
      fastify.log.error(`查询统计数据失败: ${e.message}`);
      return emptyResult;
    }
  };

  // ─── buildSseData (SSE 接口) ───
  const buildSseData = async ({ timezone: tz, type, runnerType }) => {
    // 查询用客户端时间："今天"的边界使用客户端时区
    const now = tz ? dayjs().tz(tz) : dayjs();
    const todayStart = now.startOf('day').toDate();
    const todayEnd = now.endOf('day').toDate();
    const todayStr = now.format('YYYY-MM-DD');

    // 统计查询：正在执行和等待执行的数量 + DB 查询
    const [runnerTypeStatusRows, waitingTasks] = await Promise.all([
      TaskModel.findAll({
        attributes: ['runnerType', 'status', [fn('COUNT', col('id')), 'count']],
        where: { ...(runnerType ? { runnerType } : {}) },
        group: ['runnerType', 'status'], raw: true
      }),
      TaskModel.findAll({
        attributes: ['runnerType', 'createdAt'],
        where: { status: { [Op.in]: ['pending', 'waiting'] }, ...(runnerType ? { runnerType } : {}) },
        raw: true
      })
    ]);

    const pendingByRunnerType = {};
    const waitingByRunnerType = {};
    const runningByRunnerType = {};

    const nowMs = Date.now();
    const waitingQueueMaxWaitMsByRunnerType = {};
    waitingTasks.forEach(r => {
      const rt = r.runnerType || 'manual';
      const waitMs = nowMs - new Date(r.createdAt).getTime();
      if (!waitingQueueMaxWaitMsByRunnerType[rt] || waitMs > waitingQueueMaxWaitMsByRunnerType[rt]) {
        waitingQueueMaxWaitMsByRunnerType[rt] = waitMs;
      }
    });

    const runnerTypeStats = {};
    const dbStatusCount = { pending: 0, waiting: 0, running: 0, success: 0, failed: 0, canceled: 0 };
    runnerTypeStatusRows.forEach(r => {
      if (!runnerTypeStats[r.runnerType]) runnerTypeStats[r.runnerType] = { total: 0, pending: 0, waiting: 0, waitingCount: 0, executed: 0 };
      const count = Number(r.count);
      const rt = r.runnerType || 'manual';
      runnerTypeStats[r.runnerType].total += count;
      if (r.status === 'pending') runnerTypeStats[r.runnerType].pending += count;
      if (['success', 'failed', 'canceled', 'running'].includes(r.status)) runnerTypeStats[r.runnerType].executed += count;
      if (dbStatusCount[r.status] !== undefined) dbStatusCount[r.status] += count;
      // 按 runnerType 汇总 running/waiting/pending（来自 DB 实时状态，避免 delta 脏数据）
      if (r.status === 'running') runningByRunnerType[rt] = (runningByRunnerType[rt] || 0) + count;
      if (r.status === 'waiting') waitingByRunnerType[rt] = (waitingByRunnerType[rt] || 0) + count;
      if (r.status === 'pending') pendingByRunnerType[rt] = (pendingByRunnerType[rt] || 0) + count;
      // pending 状态等同于"等待操作"，计入 waitingByRunnerType 和 runnerTypeStats.waiting/waitingCount
      if (r.status === 'pending') {
        waitingByRunnerType[rt] = (waitingByRunnerType[rt] || 0) + count;
        runnerTypeStats[r.runnerType].waiting += count;
        runnerTypeStats[r.runnerType].waitingCount += count;
      }
      if (r.status === 'waiting') {
        runnerTypeStats[r.runnerType].waiting += count;
        runnerTypeStats[r.runnerType].waitingCount += count;
      }
    });

    // Statistics 查询：已完成数据
    // 默认值：DB 实时数据（即使 statistics 服务不可用也能展示 running/waiting/pending）
    // pending 全部合并到 waiting：前端"等待操作"只有 waiting 类别，不展示独立的 pending
    const dbWaitingTotal = dbStatusCount.waiting + dbStatusCount.pending;
    let totalTasks = dbStatusCount.running + dbWaitingTotal;
    let byStatus = { success: 0, failed: 0, canceled: 0, running: dbStatusCount.running, waiting: dbWaitingTotal, pending: 0 };
    let byType = {};
    let byRunnerType = {};
    let completedToday = {};
    let completedTodayTotalDurationMsByRunnerType = {};
    let hourlyTrend = [];
    let hourlyTrendByStatus = [];
    let hourlyTrendByType = [];
    let intervalTrend = [];
    let todayDuration = {
      completedCount: 0, successCount: 0, failedCount: 0, canceledCount: 0,
      avgWaitingTime: 0, avgExecutionTime: 0, avgTotalTime: 0,
      byType: {}, byRunnerType: {}, byTypeByRunnerType: {}
    };

    try {
      const channels = await buildChannels(statisticsServices, type, runnerType);
      const parsed = await queryAndParse(statisticsServices, { channels, startTime: todayStart, endTime: todayEnd, timezone: tz, runnerType });
      if (parsed) {
        totalTasks = parsed.totalTasks + dbStatusCount.running + dbWaitingTotal;
        byStatus = { ...parsed.byStatus, running: dbStatusCount.running, waiting: dbWaitingTotal, pending: 0 };
        byType = parsed.byType;
        byRunnerType = parsed.byRunnerType;
        completedToday = { ...parsed.byRunnerType };
        todayDuration = buildDurationResult(parsed.duration);

        const hourlyArrays = buildHourlyTrendArrays(parsed);

        // SSE 格式：简化为 {hour, count}，只取当天
        hourlyTrend = hourlyArrays.hourlyTrend
          .filter(h => h.date === todayStr)
          .map(({ hour, total }) => ({ hour, count: total }));

        hourlyTrendByStatus = hourlyArrays.hourlyTrendByStatus
          .filter(h => h.date === todayStr)
          .map(({ hour, status, count }) => ({ hour, status, count }));

        hourlyTrendByType = hourlyArrays.hourlyTrendByType
          .filter(h => h.date === todayStr)
          .map(({ hour, type, count }) => ({ hour, type, count }));

        intervalTrend = hourlyTrend.map(({ hour, count }) => ({
          interval: `${String(hour).padStart(2, '0')}:00`,
          count
        }));

        for (const [rtName, rt] of Object.entries(parsed.duration.byRunnerType)) {
          completedTodayTotalDurationMsByRunnerType[rtName] = rt.sumTotal;
        }
      }
    } catch (e) {
      fastify.log.error(`SSE统计查询失败: ${e.message}`);
    }

    // 补充当前小时的 running/waiting 瞬时状态（来自 DB 实时查询，不受 statistics 服务影响）
    // 查询用客户端时间：当前小时使用客户端时区
    const currentHour = now.hour();
    if (dbStatusCount.running > 0) hourlyTrendByStatus.push({ hour: currentHour, status: 'running', count: dbStatusCount.running });
    if (dbWaitingTotal > 0) hourlyTrendByStatus.push({ hour: currentHour, status: 'waiting', count: dbWaitingTotal });

    return {
      date: todayStr,
      totalTasks,
      byStatus,
      byType,
      byRunnerType,
      pendingByRunnerType,
      waitingByRunnerType,
      runningByRunnerType,
      completedToday,
      waitingQueueMaxWaitMsByRunnerType,
      completedTodayTotalDurationMsByRunnerType,
      runnerTypeStats,
      hourlyTrend,
      hourlyTrendByStatus,
      hourlyTrendByType,
      intervalTrend,
      todayDuration
    };
  };

  // ─── sseStatistics ───
  const sseStatistics = async ({ range = '7d', timezone, type, runnerType, interval }, reply) => {
    await statisticsServices.sseStream.send(reply, {
      name: 'taskStatistics',
      params: { type, runnerType, timezone },
      fetchData: async ({ type, runnerType, timezone } = {}) => {
        try {
          return await buildSseData({ timezone, type, runnerType });
        } catch (e) {
          fastify.log.error(`SSE获取数据失败: ${e.message}`);
          return {};
        }
      },
      interval: interval || 5
    });
  };

  Object.assign(fastify[options.name].services, {
    queryStatistics,
    sseStatistics
  });
});

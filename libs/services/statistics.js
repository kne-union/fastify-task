const fp = require('fastify-plugin');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');

dayjs.extend(utc);
dayjs.extend(timezone);

const RANGE_MAP = {
  '7d': { value: 7, unit: 'day', label: '近7天' },
  '1m': { value: 1, unit: 'month', label: '近1个月' },
  '3m': { value: 3, unit: 'month', label: '近3个月' },
  '1y': { value: 1, unit: 'year', label: '近1年' }
};

const parseRange = (range = '7d') => {
  const config = RANGE_MAP[range];
  if (!config) {
    throw new Error(`不支持的时间范围: ${range}, 支持: ${Object.keys(RANGE_MAP).join(',')}`);
  }
  const now = new Date();
  const startTime = new Date(now);
  if (config.unit === 'day') {
    startTime.setDate(startTime.getDate() - config.value);
  } else if (config.unit === 'month') {
    startTime.setMonth(startTime.getMonth() - config.value);
  } else if (config.unit === 'year') {
    startTime.setFullYear(startTime.getFullYear() - config.value);
  }
  return { startTime, endTime: now, label: config.label, range };
};

const COUNT_ATTRIBUTES = ['total', 'success', 'failed', 'canceled', 'pending', 'waiting', 'running'];
const TIMING_ATTRIBUTES = ['waitingTime', 'executionTime', 'totalTime'];
const RUNNER_TYPES = ['manual', 'system'];

/** 解析通道: type:runnerType:hour */
const parseChannel = channel => {
  const parts = channel.split(':');
  return {
    type: parts[0] || null,
    runnerType: parts[1] || null,
    hour: parts[2] !== undefined ? parseInt(parts[2], 10) : null
  };
};

/** 构建不同层级的通道列表 */
const buildChannels = {
  type: types => [...types],
  runner: (types, rts = RUNNER_TYPES) => types.flatMap(t => rts.map(rt => `${t}:${rt}`)),
  hour: (types, rts = RUNNER_TYPES) => types.flatMap(t =>
    rts.flatMap(rt => Array.from({ length: 24 }, (_, h) => `${t}:${rt}:${h}`))
  )
};

/** 按 period 过滤查询结果 */
const filterByPeriod = (list, period) => (list || []).filter(item => item.period === period);

/** 从 item 中提取 sum 数据（兼容单聚合/多聚合格式） */
const getSumData = item => {
  const data = item.data || {};
  return data.sum || data;
};

/** 空的实时统计结果 */
const emptyRealtimeResult = todayDate => ({
  date: todayDate, totalTasks: 0, byStatus: {}, byType: {}, byRunnerType: {},
  hourlyTrend: [], hourlyTrendByType: [], hourlyTrendByStatus: [],
  todayDuration: null, waitingByRunnerType: {}, pendingByRunnerType: {},
  completedToday: {}, runnerTypeStats: {},
  waitingQueueMaxWaitMsByRunnerType: {}, completedTodayTotalDurationMsByRunnerType: {}
});

/** 空的历史统计结果 */
const emptyHistoryResult = () => ({
  totalTasks: 0, byStatus: {}, byType: {},
  recentTrend: [], recentTrendByType: [], durationTrend: [], hourlyCompletionTrend: []
});

module.exports = fp(async (fastify, options) => {
  const statisticsServices = fastify[`${options.name}Statistics`].services;

  /** 获取所有任务类型（通过 channelMeta 根通道，无冒号） */
  const getTaskTypes = async () => {
    const metas = await statisticsServices.channelMeta.list();
    return metas.map(m => m.channel).filter(ch => ch && !ch.includes(':'));
  };

  /**
   * 实时统计：通过统计模块查询今日数据
   */
  const queryRealtimeData = async ({ timezone }) => {
    const tz = timezone || 'UTC';
    const now = dayjs().tz(tz);
    const todayStart = now.startOf('day').toDate();
    const todayDate = now.format('YYYY-MM-DD');

    const taskTypes = await getTaskTypes();
    if (taskTypes.length === 0) return emptyRealtimeResult(todayDate);

    const typeChannels = buildChannels.type(taskTypes);
    const runnerChannels = buildChannels.runner(taskTypes);
    const hourChannels = buildChannels.hour(taskTypes);

    // 1. 按类型查询计数汇总
    const typeCountResult = await statisticsServices.query({
      channels: typeChannels, startTime: todayStart, endTime: now.toDate(),
      attributeNames: COUNT_ATTRIBUTES, aggregates: ['sum'], timezone: tz
    });

    // 2. 按执行方式查询计数
    const runnerCountResult = await statisticsServices.query({
      channels: runnerChannels, startTime: todayStart, endTime: now.toDate(),
      attributeNames: COUNT_ATTRIBUTES, aggregates: ['sum'], timezone: tz
    });

    // 3. 按小时通道查询计数趋势
    const hourCountResult = await statisticsServices.query({
      channels: hourChannels, startTime: todayStart, endTime: now.toDate(),
      attributeNames: COUNT_ATTRIBUTES, aggregates: ['sum'], timezone: tz
    });

    // 汇总 byStatus / byType / totalTasks
    const byStatus = {};
    const byType = {};
    let totalTasks = 0;
    for (const item of typeCountResult.list || []) {
      const sumData = getSumData(item);
      if (sumData.total !== undefined) {
        byType[item.channel] = Math.round(sumData.total);
        totalTasks += Math.round(sumData.total);
      }
      for (const status of COUNT_ATTRIBUTES) {
        if (status !== 'total' && sumData[status] !== undefined) {
          byStatus[status] = (byStatus[status] || 0) + Math.round(sumData[status]);
        }
      }
    }

    // 汇总 byRunnerType / waiting / pending / completed / runnerTypeStats
    const byRunnerType = {};
    const waitingByRunnerType = {};
    const pendingByRunnerType = {};
    const completedToday = {};
    const runnerTypeStats = {};
    for (const item of runnerCountResult.list || []) {
      const { runnerType } = parseChannel(item.channel);
      if (!runnerType) continue;
      const sumData = getSumData(item);
      if (sumData.total !== undefined) {
        byRunnerType[runnerType] = (byRunnerType[runnerType] || 0) + Math.round(sumData.total);
      }
      if (!runnerTypeStats[runnerType]) runnerTypeStats[runnerType] = { total: 0, waiting: 0, waitingCount: 0 };
      if (sumData.total !== undefined) runnerTypeStats[runnerType].total += Math.round(sumData.total);
      if (sumData.waiting !== undefined) {
        const w = Math.round(sumData.waiting);
        waitingByRunnerType[runnerType] = w;
        runnerTypeStats[runnerType].waiting += w;
        runnerTypeStats[runnerType].waitingCount += w;
      }
      if (sumData.pending !== undefined) pendingByRunnerType[runnerType] = Math.round(sumData.pending);
      if (sumData.success !== undefined) completedToday[runnerType] = Math.round(sumData.success);
    }

    // 按小时趋势（直接从 channel 中的小时数分组，用 time 字段做时区转换）
    const hourBuckets = {};
    for (const item of hourCountResult.list || []) {
      const { hour: channelHour, type: taskType } = parseChannel(item.channel);
      if (channelHour === null || isNaN(channelHour)) continue;
      // 使用 time 字段获取本地小时（更精确的时区处理）
      const localHour = item.time ? dayjs(item.time).tz(tz).hour() : channelHour;
      const sumData = getSumData(item);
      const count = sumData.total !== undefined ? Math.round(sumData.total) : 0;

      if (!hourBuckets[localHour]) hourBuckets[localHour] = { total: 0, byType: {}, byStatus: {} };
      const bucket = hourBuckets[localHour];
      bucket.total += count;
      if (taskType) bucket.byType[taskType] = (bucket.byType[taskType] || 0) + count;
      for (const status of COUNT_ATTRIBUTES) {
        if (status !== 'total' && sumData[status] !== undefined) {
          bucket.byStatus[status] = (bucket.byStatus[status] || 0) + Math.round(sumData[status]);
        }
      }
    }

    const hourlyTrend = [], hourlyTrendByType = [], hourlyTrendByStatus = [];
    for (const [h, bucket] of Object.entries(hourBuckets)) {
      const hour = parseInt(h, 10);
      if (bucket.total > 0) hourlyTrend.push({ hour, count: bucket.total });
      for (const [type, count] of Object.entries(bucket.byType)) hourlyTrendByType.push({ hour, type, count });
      for (const [status, count] of Object.entries(bucket.byStatus)) hourlyTrendByStatus.push({ hour, status, count });
    }

    // 时长统计（avg 聚合）
    const durationResult = await statisticsServices.query({
      channels: runnerChannels, startTime: todayStart, endTime: now.toDate(),
      attributeNames: TIMING_ATTRIBUTES, aggregates: ['avg'], timezone: tz
    });
    let totalAvgExec = 0, totalAvgWait = 0, totalAvgTotal = 0, durationCount = 0;
    for (const item of durationResult.list || []) {
      const avgData = item.data?.avg || item.data || {};
      if (avgData.totalTime !== undefined) {
        totalAvgExec += avgData.executionTime || 0;
        totalAvgWait += avgData.waitingTime || 0;
        totalAvgTotal += avgData.totalTime || 0;
        durationCount++;
      }
    }
    const todayDuration = durationCount > 0 ? {
      avgExecutionTime: Math.round(totalAvgExec / durationCount),
      avgWaitingTime: Math.round(totalAvgWait / durationCount),
      avgTotalTime: Math.round(totalAvgTotal / durationCount)
    } : null;

    // 等待队列最长等待时间（max 聚合）
    const maxWaitResult = await statisticsServices.query({
      channels: runnerChannels, startTime: todayStart, endTime: now.toDate(),
      attributeNames: ['waitingTime'], aggregates: ['max'], timezone: tz
    });
    const waitingQueueMaxWaitMsByRunnerType = {};
    for (const item of maxWaitResult.list || []) {
      const { runnerType } = parseChannel(item.channel);
      if (!runnerType) continue;
      const maxData = item.data?.max || item.data || {};
      if (maxData.waitingTime !== undefined) {
        const v = Math.round(maxData.waitingTime);
        if (!waitingQueueMaxWaitMsByRunnerType[runnerType] || v > waitingQueueMaxWaitMsByRunnerType[runnerType]) {
          waitingQueueMaxWaitMsByRunnerType[runnerType] = v;
        }
      }
    }

    // 当日完成任务的创建→完成总耗时（sum 聚合）
    const totalDurationResult = await statisticsServices.query({
      channels: runnerChannels, startTime: todayStart, endTime: now.toDate(),
      attributeNames: ['totalTime'], aggregates: ['sum'], timezone: tz
    });
    const completedTodayTotalDurationMsByRunnerType = {};
    for (const item of totalDurationResult.list || []) {
      const { runnerType } = parseChannel(item.channel);
      if (!runnerType) continue;
      const sumData = getSumData(item);
      if (sumData.totalTime !== undefined) {
        completedTodayTotalDurationMsByRunnerType[runnerType] =
          (completedTodayTotalDurationMsByRunnerType[runnerType] || 0) + Math.round(sumData.totalTime);
      }
    }

    return {
      date: todayDate, totalTasks, byStatus, byType, byRunnerType,
      hourlyTrend, hourlyTrendByType, hourlyTrendByStatus, todayDuration,
      waitingByRunnerType, pendingByRunnerType, completedToday, runnerTypeStats,
      waitingQueueMaxWaitMsByRunnerType, completedTodayTotalDurationMsByRunnerType
    };
  };

  /**
   * 历史统计：通过统计模块查询
   */
  const queryHistoryData = async ({ range = '7d', timezone, type, runnerType }) => {
    const { startTime, endTime } = parseRange(range);
    const tz = timezone || undefined;

    const taskTypes = type ? [type] : await getTaskTypes();
    const runnerTypes = runnerType ? [runnerType] : RUNNER_TYPES;

    if (taskTypes.length === 0) return emptyHistoryResult();

    const typeChannels = buildChannels.type(taskTypes);
    const runnerChannels = buildChannels.runner(taskTypes, runnerTypes);
    const hourChannels = buildChannels.hour(taskTypes, runnerTypes);

    // 按类型查询计数汇总
    const typeCountResult = await statisticsServices.query({
      channels: typeChannels, startTime, endTime,
      attributeNames: COUNT_ATTRIBUTES, aggregates: ['sum'], timezone: tz
    });

    // 按小时通道查询计数趋势
    const hourCountResult = await statisticsServices.query({
      channels: hourChannels, startTime, endTime,
      attributeNames: COUNT_ATTRIBUTES, aggregates: ['sum'], timezone: tz
    });

    // 汇总
    const byStatus = {};
    const byType = {};
    let totalTasks = 0;
    for (const item of typeCountResult.list || []) {
      const sumData = getSumData(item);
      if (sumData.total !== undefined) {
        byType[item.channel] = Math.round(sumData.total);
        totalTasks += Math.round(sumData.total);
      }
      for (const status of COUNT_ATTRIBUTES) {
        if (status !== 'total' && sumData[status] !== undefined) {
          byStatus[status] = (byStatus[status] || 0) + Math.round(sumData[status]);
        }
      }
    }

    // 按日聚合趋势（从小时通道数据中按 time 日期分组）
    const dateMap = {};
    for (const item of hourCountResult.list || []) {
      if (!item.time) continue;
      const dateStr = tz ? dayjs(item.time).tz(tz).format('YYYY-MM-DD') : dayjs(item.time).format('YYYY-MM-DD');
      const sumData = getSumData(item);
      const { type: taskType } = parseChannel(item.channel);
      const count = sumData.total !== undefined ? Math.round(sumData.total) : 0;

      if (!dateMap[dateStr]) dateMap[dateStr] = { total: 0, byType: {} };
      dateMap[dateStr].total += count;
      if (taskType) dateMap[dateStr].byType[taskType] = (dateMap[dateStr].byType[taskType] || 0) + count;
    }

    const recentTrend = Object.entries(dateMap)
      .map(([date, data]) => ({ date, count: data.total }))
      .sort((a, b) => a.date.localeCompare(b.date));

    const recentTrendByType = [];
    for (const [date, data] of Object.entries(dateMap)) {
      for (const [t, count] of Object.entries(data.byType)) {
        recentTrendByType.push({ date, type: t, count });
      }
    }
    recentTrendByType.sort((a, b) => a.date.localeCompare(b.date) || a.type.localeCompare(b.type));

    // hourlyCompletionTrend（按日期+小时聚合，小时从 channel 解析）
    const hourDayMap = {};
    for (const item of hourCountResult.list || []) {
      if (!item.time) continue;
      const dj = tz ? dayjs(item.time).tz(tz) : dayjs(item.time);
      const date = dj.format('YYYY-MM-DD');
      const { hour } = parseChannel(item.channel);
      if (hour === null || isNaN(hour)) continue;
      const key = `${date}|${hour}`;
      if (!hourDayMap[key]) hourDayMap[key] = { date, hour, byStatus: {}, byType: {}, total: 0 };

      const bucket = hourDayMap[key];
      const sumData = getSumData(item);
      const { type: taskType } = parseChannel(item.channel);
      if (sumData.total !== undefined) bucket.total += Math.round(sumData.total);
      for (const status of COUNT_ATTRIBUTES) {
        if (status !== 'total' && sumData[status] !== undefined) {
          bucket.byStatus[status] = (bucket.byStatus[status] || 0) + Math.round(sumData[status]);
        }
      }
      if (taskType && sumData.total !== undefined) {
        bucket.byType[taskType] = (bucket.byType[taskType] || 0) + Math.round(sumData.total);
      }
    }

    const hourlyCompletionTrend = Object.values(hourDayMap).map(b => ({
      date: b.date, hour: b.hour, totalCompleted: b.total,
      ...b.byStatus,
      ...Object.fromEntries(Object.entries(b.byType).map(([t, c]) => [`${t}Count`, c]))
    }));

    // 时长趋势（按日 + runnerType 聚合平均执行/等待时长）
    const durationResult = await statisticsServices.query({
      channels: runnerChannels, startTime, endTime,
      attributeNames: TIMING_ATTRIBUTES, aggregates: ['avg', 'sum', 'count'], timezone: tz
    });

    const durationByDate = {};
    for (const item of durationResult.list || []) {
      if (!item.time) continue;
      const dateStr = tz ? dayjs(item.time).tz(tz).format('YYYY-MM-DD') : dayjs(item.time).format('YYYY-MM-DD');
      const { runnerType: rt } = parseChannel(item.channel);
      if (!rt) continue;

      const data = item.data || {};
      const sumData = data.sum || {};
      const countData = data.count || {};
      const avgData = data.avg || {};

      let avgExecutionTime = 0, avgWaitingTime = 0;
      if (countData.totalTime > 0 && sumData.executionTime !== undefined) {
        avgExecutionTime = Math.round(sumData.executionTime / countData.totalTime);
        avgWaitingTime = Math.round((sumData.waitingTime || 0) / countData.totalTime);
      } else if (avgData.executionTime !== undefined) {
        avgExecutionTime = Math.round(avgData.executionTime);
        avgWaitingTime = Math.round(avgData.waitingTime || 0);
      }

      if (avgExecutionTime > 0 || avgWaitingTime > 0) {
        if (!durationByDate[dateStr]) durationByDate[dateStr] = {};
        durationByDate[dateStr][rt] = { avgExecutionTime, avgWaitingTime };
      }
    }

    const durationTrend = Object.entries(durationByDate)
      .map(([date, byRunnerType]) => ({ date, byRunnerType }))
      .sort((a, b) => a.date.localeCompare(b.date));

    return {
      totalTasks, byStatus, byType,
      recentTrend, recentTrendByType, durationTrend, hourlyCompletionTrend
    };
  };

  /** HTTP 接口：历史统计概览 */
  const queryStatistics = async ({ range = '7d', timezone, type, runnerType }) => {
    return await queryHistoryData({ range, timezone, type, runnerType });
  };

  /** SSE 接口：实时统计推送 */
  const sseStatistics = async ({ timezone, interval }, reply) => {
    await statisticsServices.sseStream.send(reply, {
      name: 'query', params: { timezone },
      fetchData: async () => await queryRealtimeData({ timezone }),
      interval: interval || 5
    });
  };

  Object.assign(fastify[options.name].services, { queryStatistics, sseStatistics });
});

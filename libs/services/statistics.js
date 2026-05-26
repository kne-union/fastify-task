const fp = require('fastify-plugin');

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

const buildChannel = ({ type, runnerType }) => {
  if (type && runnerType) return `task:${type}:${runnerType}`;
  if (type) return `task:${type}`;
  return 'task';
};

module.exports = fp(async (fastify, options) => {
  const statisticsServices = fastify[`${options.name}Statistics`].services;

  const queryStatistics = async ({ range = '7d', timezone, type, runnerType }) => {
    const { startTime, endTime } = parseRange(range);
    const channel = buildChannel({ type, runnerType });
    const includeChildren = !type && !runnerType;

    return await statisticsServices.query({
      channels: [channel],
      startTime,
      endTime,
      timezone: timezone || undefined,
      includeChildren
    });
  };

  const sseStatistics = async ({ range = '7d', timezone, type, runnerType, interval }, reply) => {
    const channel = buildChannel({ type, runnerType });
    const includeChildren = !type && !runnerType;

    await statisticsServices.sseStream.send(reply, {
      name: 'query',
      params: { channels: channel, timezone, includeChildren },
      fetchData: async () => {
        const { startTime, endTime } = parseRange(range);
        return statisticsServices.query({
          channels: [channel],
          startTime,
          endTime,
          timezone: timezone || undefined,
          includeChildren
        });
      },
      interval: interval || 5
    });
  };

  Object.assign(fastify[options.name].services, {
    queryStatistics,
    sseStatistics
  });
});

const fp = require('fastify-plugin');
const { success, statistics } = require('../schema/response');

const TAG = '任务-统计';

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.get(
    `${options.prefix}/statistics`,
    {
      onRequest: options.getAuthenticate('statistics'),
      schema: {
        tags: [TAG],
        summary: '获取任务统计概览',
        description: '按时间范围汇总已结束任务的数量、状态/类型/执行方式分布、每日与按小时趋势及耗时；不支持的 range 报错，其他查询异常时返回空统计',
        query: {
          type: 'object',
          properties: {
            range: { type: 'string', default: '7d', description: '时间范围: 7d=近7天, 1m=近1个月, 3m=近3个月, 1y=近1年' },
            timezone: { type: 'string', description: '时区，如 Asia/Shanghai，默认服务器时区' },
            type: { type: 'string', description: '按任务类型筛选' },
            runnerType: { type: 'string', description: '按执行方式筛选 manual / system' }
          }
        },
        response: success('任务统计数据', statistics)
      }
    },
    async request => {
      const { range = '7d', timezone, type, runnerType } = request.query;
      return await services.queryStatistics({ range, timezone, type, runnerType });
    }
  );

  // SSE 实时统计数据推送
  fastify.get(
    `${options.prefix}/statistics/sse`,
    {
      onRequest: options.getAuthenticate('statistics'),
      schema: {
        tags: [TAG],
        summary: 'SSE实时推送任务统计数据',
        description: '以 text/event-stream 长连接按 interval 推送实时统计（当天各状态数量、执行中/等待中任务数等），客户端断开后停止',
        query: {
          type: 'object',
          properties: {
            range: { type: 'string', default: '7d', description: '时间范围: 7d=近7天, 1m=近1个月, 3m=近3个月, 1y=近1年' },
            timezone: { type: 'string', description: '时区，如 Asia/Shanghai，默认服务器时区' },
            type: { type: 'string', description: '按任务类型筛选' },
            runnerType: { type: 'string', description: '按执行方式筛选 manual / system' },
            interval: { type: 'integer', minimum: 1, default: 5, description: '推送间隔时间（秒），最小1秒，默认5秒' }
          }
        },
        response: { 200: { description: 'text/event-stream 事件流，每条 data 为实时统计 JSON' } }
      }
    },
    async (request, reply) => {
      const { range = '7d', timezone, type, runnerType, interval } = request.query;
      await services.sseStatistics({ range, timezone, type, runnerType, interval }, reply);
    }
  );
});

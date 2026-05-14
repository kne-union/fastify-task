const fp = require('fastify-plugin');

/** 看板统计：概览 GET /statistics 与 SSE /statistics/sse（独立 controller 插件，见业务插件开发指南「路由标准化」） */
module.exports = fp(
  async (fastify, options) => {
    const { services } = fastify[options.name];

  fastify.get(
    `${options.prefix}/statistics`,
    {
      onRequest: options.getAuthenticate('statistics'),
      schema: {
        summary: '获取任务统计概览',
        query: {
          type: 'object',
          properties: {
            range: { type: 'string', default: '7d', description: '时间范围: 7d=近7天, 1m=近1个月, 1y=近1年' },
            timezone: { type: 'string', description: '时区，如 Asia/Shanghai，默认服务器时区' },
            type: { type: 'string', description: '按任务类型筛选' },
            runnerType: { type: 'string', description: '按执行方式筛选 manual / system' }
          }
        },
        response: {
          200: {
            type: 'object',
            properties: {
              range: { type: 'string', description: '当前时间范围' },
              rangeLabel: { type: 'string', description: '时间范围描述' },
              totalTasks: { type: 'integer', description: '时间范围内任务总数' },
              byStatus: {
                type: 'object',
                description: '按任务状态统计',
                additionalProperties: { type: 'integer' }
              },
              byType: {
                type: 'object',
                description: '按任务类型统计',
                additionalProperties: { type: 'integer' }
              },
              byRunnerType: {
                type: 'object',
                description: '按执行者类型统计(manual/system)',
                additionalProperties: { type: 'integer' }
              },
              byTargetType: {
                type: 'object',
                description: '按目标对象类型统计',
                additionalProperties: { type: 'integer' }
              },
              recentTrend: {
                type: 'array',
                description: '按日任务创建趋势',
                items: {
                  type: 'object',
                  properties: {
                    date: { type: 'string', description: '日期' },
                    count: { type: 'integer', description: '任务数量' }
                  }
                }
              },
              recentTrendByStatus: {
                type: 'array',
                description: '按日+状态任务创建趋势',
                items: {
                  type: 'object',
                  properties: {
                    date: { type: 'string', description: '日期' },
                    status: { type: 'string', description: '任务状态' },
                    count: { type: 'integer', description: '任务数量' }
                  }
                }
              },
              recentTrendByType: {
                type: 'array',
                description: '按日+类型任务创建趋势',
                items: {
                  type: 'object',
                  properties: {
                    date: { type: 'string', description: '日期' },
                    type: { type: 'string', description: '任务类型' },
                    count: { type: 'integer', description: '任务数量' }
                  }
                }
              },
              durationTrend: {
                type: 'array',
                description: '按日平均耗时趋势(来自预聚合统计表)',
                items: {
                  type: 'object',
                  properties: {
                    date: { type: 'string', description: '完成日期' },
                    completedCount: { type: 'integer', description: '完成任务数' },
                    successCount: { type: 'integer', description: '成功数' },
                    failedCount: { type: 'integer', description: '失败数' },
                    canceledCount: { type: 'integer', description: '取消数' },
                    avgWaitingTime: { type: 'integer', description: '平均等待时间(毫秒)' },
                    avgExecutionTime: { type: 'integer', description: '平均执行时间(毫秒)' },
                    avgTotalTime: { type: 'integer', description: '平均总耗时(毫秒)' },
                    byType: {
                      type: 'object',
                      description: '按类型分组的耗时统计',
                      additionalProperties: {
                        type: 'object',
                        properties: {
                          count: { type: 'integer' },
                          avgWaitingTime: { type: 'integer' },
                          avgExecutionTime: { type: 'integer' },
                          avgTotalTime: { type: 'integer' }
                        }
                      }
                    },
                    byRunnerType: {
                      type: 'object',
                      description: '按执行者类型分组的耗时统计',
                      additionalProperties: {
                        type: 'object',
                        properties: {
                          count: { type: 'integer' },
                          avgWaitingTime: { type: 'integer' },
                          avgExecutionTime: { type: 'integer' },
                          avgTotalTime: { type: 'integer' }
                        }
                      }
                    }
                  }
                }
              },
              hourlyCompletionTrend: {
                type: 'array',
                description: '按完成时刻归入 UTC 小时桶后，在请求 timezone 下展开的每小时+类型+执行方式完成数',
                items: {
                  type: 'object',
                  properties: {
                    date: { type: 'string', description: '客户端时区下的日期 YYYY-MM-DD' },
                    hour: { type: 'integer', description: '客户端时区下的小时 0-23' },
                    type: { type: 'string' },
                    runnerType: { type: 'string' },
                    totalCompleted: { type: 'integer' },
                    successCount: { type: 'integer' },
                    failedCount: { type: 'integer' },
                    canceledCount: { type: 'integer' }
                  }
                }
              }
            }
          }
        }
      }
    },
    async request => {
      const { range = '7d', timezone, type, runnerType } = request.query;
      return await services.statistics.getOverview({ range, timezone, type, runnerType });
    }
  );

  // SSE 实时统计数据推送
  fastify.get(
    `${options.prefix}/statistics/sse`,
    {
      sse: true,
      onRequest: options.getAuthenticate('statistics'),
      schema: {
        summary: 'SSE实时推送当天任务统计数据',
        query: {
          type: 'object',
          properties: {
            interval: { type: 'integer', minimum: 1, default: 5, description: '推送间隔时间（秒），最小1秒，默认5秒' },
            timezone: { type: 'string', description: '时区，如 Asia/Shanghai，默认服务器时区' },
            type: { type: 'string', description: '按任务类型筛选' },
            runnerType: { type: 'string', description: '按执行方式筛选 manual / system' }
          }
        }
      }
    },
    async function (request, reply) {
      const intervalSeconds = request.query.interval;
      const { timezone, type, runnerType } = request.query;
      reply.sse.keepAlive();

      async function* eventStream() {
        while (reply.sse.isConnected) {
          try {
            yield { data: JSON.stringify(await services.statistics.getRealtime({ timezone, type, runnerType })) };
          } catch (err) {
            yield { event: 'error', data: JSON.stringify({ message: err.message }) };
          }
          await new Promise(resolve => setTimeout(resolve, intervalSeconds * 1000));
        }
      }

      await reply.sse.send(eventStream());
    }
  );
  });

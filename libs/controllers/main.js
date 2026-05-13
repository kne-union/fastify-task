const fp = require('fastify-plugin');

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];
  fastify.get(
    `${options.prefix}/list`,
    {
      onRequest: options.getAuthenticate('read'),
      schema: {
        summary: '获取任务列表',
        query: {
          type: 'object',
          properties: {
            perPage: {
              type: 'number',
              default: 20
            },
            currentPage: {
              type: 'number',
              default: 1
            },
            filter: {
              type: 'object',
              properties: {
                id: {
                  type: 'string'
                },
                targetId: {
                  type: 'string'
                },
                targetName: {
                  type: 'string'
                },
                type: {
                  type: 'string'
                },
                status: {
                  type: 'string'
                },
                runnerType: {
                  type: 'string'
                },
                createdAt: {
                  type: 'object',
                  properties: {
                    startTime: {
                      type: 'string'
                    },
                    endTime: {
                      type: 'string'
                    }
                  },
                  description: 'createdAt区间查找'
                },
                completedAt: {
                  type: 'object',
                  properties: {
                    startTime: {
                      type: 'string'
                    },
                    endTime: {
                      type: 'string'
                    }
                  },
                  description: 'completedAt区间查找'
                }
              }
            },
            sort: {
              type: 'object',
              description: '按completedAt、updatedAt字段排序，ASC为升序，DESC为降序'
            }
          }
        }
      }
    },
    async request => {
      return services.list(request.query);
    }
  );

  fastify.post(
    `${options.prefix}/complete`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        summary: '手动完成任务',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string'
            },
            status: {
              type: 'string',
              enum: ['success', 'failed']
            },
            error: {
              type: 'string'
            },
            msg: {
              type: 'string'
            },
            output: {
              type: 'object'
            }
          },
          required: ['id', 'status']
        }
      }
    },
    async request => {
      await services.complete(Object.assign({}, request.body, { userId: request.userInfo?.id }));
      return {};
    }
  );

  fastify.post(
    `${options.prefix}/cancel`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        summary: '取消任务',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string'
            }
          }
        }
      }
    },
    async request => {
      await services.cancel(request.body);
      return {};
    }
  );

  fastify.post(
    `${options.prefix}/retry`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        summary: '重试任务',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string'
            },
            taskIds: {
              type: 'array',
              items: {
                type: 'string'
              }
            }
          }
        }
      }
    },
    async request => {
      await services.retry(request.body);
      return {};
    }
  );

  fastify.post(
    `${options.prefix}/next`,
    {
      schema: {
        summary: '处理任务next',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID'
            },
            signature: {
              type: 'string',
              description: '签名'
            },
            result: {
              type: 'string',
              description: '结果'
            }
          }
        }
      }
    },
    async request => {
      return await services.processNext(request.body);
    }
  );

  fastify.post(
    `${options.prefix}/log`,
    {
      schema: {
        summary: '记录任务日志',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string'
            },
            data: {
              type: 'object'
            },
            message: {
              type: 'string'
            },
            signature: {
              type: 'string',
              description: 'HMAC-SHA256签名'
            }
          }
        },
        query: {
          type: 'object',
          properties: {
            taskId: {
              type: 'string'
            }
          }
        }
      }
    },
    async request => {
      return await services.logWithSignature(Object.assign({}, request.body, { taskId: request.query.taskId }));
    }
  );

  fastify.post(
    `${options.prefix}/callback`,
    {
      schema: {
        summary: '任务回调',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string'
            },
            code: {
              type: 'number'
            },
            data: {
              type: 'object'
            },
            message: {
              type: 'string'
            },
            signature: {
              type: 'string',
              description: 'HMAC-SHA256签名'
            }
          }
        },
        query: {
          type: 'object',
          properties: {
            taskId: {
              type: 'string'
            }
          }
        }
      }
    },
    async request => {
      return await services.callbackWithSignature(Object.assign({}, request.body, { taskId: request.query.taskId }));
    }
  );

  // 历史统计数据
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
            type: { type: 'string', description: '按任务类型筛选' }
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
              }
            }
          }
        }
      }
    },
    async request => {
      const { range = '7d', timezone, type } = request.query;
      return await services.statistics.getOverview({ range, timezone, type });
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
            type: { type: 'string', description: '按任务类型筛选' }
          }
        }
      }
    },
    async function (request, reply) {
      const intervalSeconds = request.query.interval;
      const { timezone, type } = request.query;
      reply.sse.keepAlive();

      async function* eventStream() {
        while (reply.sse.isConnected) {
          try {
            yield { data: JSON.stringify(await services.statistics.getRealtime({ timezone, type })) };
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

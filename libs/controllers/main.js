const fp = require('fastify-plugin');

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];
  // #14: 新增创建任务的 REST 接口
  fastify.post(
    `${options.prefix}/create`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        summary: '创建任务',
        body: {
          type: 'object',
          properties: {
            type: {
              type: 'string',
              description: '任务类型'
            },
            targetId: {
              type: 'string',
              description: '目标对象ID'
            },
            targetType: {
              type: 'string',
              description: '目标对象类型'
            },
            input: {
              type: 'object',
              description: '输入数据'
            },
            runnerType: {
              type: 'string',
              enum: ['manual', 'system'],
              description: '执行者类型'
            },
            delay: {
              type: 'number',
              description: '延迟执行秒数'
            },
            scriptName: {
              type: 'string',
              description: '任务脚本名称'
            },
            priority: {
              type: 'number',
              description: '优先级，数值越大越优先，默认0'
            },
            parentTaskId: {
              type: 'string',
              description: '父任务ID，用于任务依赖'
            },
            maxRetries: {
              type: 'number',
              description: '最大自动重试次数，默认0'
            }
          },
          required: ['type', 'targetId', 'targetType']
        }
      }
    },
    async request => {
      const task = await services.create(Object.assign({}, request.body, { userId: request.userInfo?.id }));
      return { id: task.id };
    }
  );

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
              type: 'string',
              description: '任务ID'
            },
            targetId: {
              type: 'string',
              description: '目标对象ID（批量取消时使用）'
            },
            targetType: {
              type: 'string',
              description: '目标对象类型（批量取消时使用）'
            },
            type: {
              type: 'string',
              description: '任务类型（批量取消时使用）'
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
});

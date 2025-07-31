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
              type: 'object'
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
});

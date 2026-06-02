const fp = require('fastify-plugin');
const path = require('node:path');
const { getConfigManager } = require('./libs/utils/config');

module.exports = fp(
  async (fastify, options) => {
    // 初始化配置管理器，合并默认配置和用户配置
    const configManager = getConfigManager(options);
    options = configManager.getAll();

    fastify.register(require('@kne/fastify-statistics'), {
      dbTableNamePrefix: options.dbTableNamePrefix,
      name: `${options.name}Statistics`
    });

    fastify.register(require('@kne/fastify-namespace'), {
      options,
      name: options.name,
      modules: [
        ['controllers', path.resolve(__dirname, './libs/controllers')],
        [
          'models',
          await fastify.sequelize.addModels(path.resolve(__dirname, './libs/models'), {
            prefix: options.dbTableNamePrefix,
            getUserModel: options.getUserModel,
            modelPrefix: options.name
          })
        ],
        ['services', path.resolve(__dirname, './libs/services')]
      ]
    });

    fastify.register(
      require('fastify-plugin')(async fastify => {
        if (options.cronTime) {
          fastify.cron.createJob({
            cronTime: options.cronTime,
            onTick: async () => {
              const { runner } = fastify[options.name].services;
              await runner();
            },
            start: true
          });
          //启动时，将running状态的任务设置为pending
          fastify.addHook('onReady', async () => {
            await fastify[options.name].services.resetAll();
          });
        }
      })
    );
  },
  {
    name: 'fastify-task',
    dependencies: ['fastify-cron']
  }
);

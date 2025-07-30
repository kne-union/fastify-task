const fp = require('fastify-plugin');
const path = require('node:path');

module.exports = fp(
  async (fastify, options) => {
    options = Object.assign(
      {},
      {
        dbTableNamePrefix: 't_',
        prefix: '/api/task',
        name: 'task',
        limit: 10,
        dir: path.resolve(process.cwd(), 'libs', 'tasks'),
        syncCron: '*/10 * * * *',
        scriptName: 'index',
        maxPollTimes: 100,
        pollInterval: 5000,
        getUserModel: () => {
          return fastify.account.models.user;
        },
        getAuthenticate: () => {
          return [fastify.account.authenticate.user, fastify.account.authenticate.admin];
        },
        task: {}
      },
      options
    );

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
        if (options.syncCron) {
          fastify.cron.createJob({
            cronTime: options.syncCron,
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

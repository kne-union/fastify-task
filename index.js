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
        cronTime: '*/10 * * * *',
        scriptName: 'index',
        maxPollTimes: 20,
        pollInterval: 10000,
        /** 每小时任务完成按 UTC 桶重算（默认每时第 5 分执行，聚合上一完整 UTC 小时）；传 null/false 关闭 */
        hourlyStatisticsCronTime: '5 * * * *',
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

    fastify.register(require('@fastify/sse'));

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
            fastify[options.name].services.syncHourlyStatisticsFromTasks().catch(console.error);
          });
        }
        if (options.hourlyStatisticsCronTime) {
          fastify.cron.createJob({
            cronTime: options.hourlyStatisticsCronTime,
            onTick: async () => {
              const { syncHourlyStatisticsFromTasks } = fastify[options.name].services;
              await syncHourlyStatisticsFromTasks();
            },
            start: true
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

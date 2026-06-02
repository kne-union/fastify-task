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
        /** 任务目录列表，运行时可通过 addDir 动态添加 */
        dirs: null,
        cronTime: '*/10 * * * *',
        scriptName: 'index',
        maxPollTimes: 20,
        pollInterval: 10000,
        /** 任务执行超时时间（毫秒），0 表示不超时，默认 1800000ms */
        taskTimeout: 30 * 60 * 1000,
        /** 启动时仅恢复超出该时间窗口的 running 任务，避免多实例重启误重置正在执行的任务 */
        recoverRunningTaskAfter: null,
        /** 是否在启动时恢复陈旧 running 任务 */
        recoverRunningTasksOnStart: true,
        /** 重试基础延迟（毫秒），实际延迟 = retryBaseDelay * 2^(retryCount-1) */
        retryBaseDelay: 5000,
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

    if (options.recoverRunningTaskAfter == null) {
      options.recoverRunningTaskAfter = options.taskTimeout > 0 ? options.taskTimeout : 30 * 60 * 1000;
    }

    // 初始化 dirs：优先使用用户传入的 dirs，否则以 dir 为默认值，保证向后兼容
    if (!options.dirs) {
      options.dirs = [options.dir];
    } else if (!options.dirs.includes(options.dir)) {
      options.dirs = [options.dir, ...options.dirs];
    }

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
          // 启动时只恢复陈旧 running 任务，避免多实例部署时重置其他实例正在执行的任务
          fastify.addHook('onReady', async () => {
            if (options.recoverRunningTasksOnStart) {
              await fastify[options.name].services.resetAll({ staleOnly: true });
            }
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

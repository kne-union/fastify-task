const fp = require('fastify-plugin');
const path = require('node:path');
const fs = require('fs-extra');

module.exports = fp(async (fastify, options) => {
  const { models } = fastify[options.name];
  const { Op } = fastify.sequelize.Sequelize;
  const create = async ({ userId, input, type, targetId, targetType, runnerType, delay, scriptName, options: currentOptions }) => {
    if (typeof options.task[type] !== 'function') {
      throw new Error('未找到合法的任务声明');
    }
    return await models.task.create({
      userId,
      input,
      type,
      targetId,
      targetType,
      runnerType,
      startTime: delay > 0 ? new Date(Date.now() + 1000 * delay) : new Date(),
      scriptName,
      options: currentOptions
    });
  };

  const resetAll = async () => {
    await models.task.update(
      {
        status: 'pending'
      },
      {
        where: {
          status: 'running'
        }
      }
    );
  };

  const processSystemTask = async task => {
    const taskModulePath = path.resolve(options.dir, task.type, `${task.scriptName || options.scriptName}.js`);
    if (!(await fs.exists(taskModulePath))) {
      await task.update({
        status: 'failed',
        error: `未匹配到任务执行器:${taskModulePath}`
      });
      return;
    }
    await task.update({
      status: 'running',
      progress: 0
    });
    require(taskModulePath)(fastify, options, {
      task,
      progress: async progress => {
        await task.update({
          progress
        });
      },
      polling: async (callback, currentOptions) => {
        let pollCount = 0;
        const maxPollTimes = currentOptions?.maxPollTimes || options.maxPollTimes;
        const pollInterval = currentOptions?.pollInterval || options.pollInterval;
        return await new Promise((resolve, reject) => {
          const timer = setInterval(async () => {
            pollCount++;
            if (pollCount > maxPollTimes) {
              clearInterval(timer);
              reject(`轮询超时（${maxPollTimes}次），任务未完成`);
            }
            const { result, data, message } = Object.assign({}, await callback());
            if (result === 'failed') {
              clearInterval(timer);
              reject(`任务处理失败:${message}`);
            }
            if (result === 'success') {
              clearInterval(timer);
              resolve(data);
            }
          }, pollInterval);
        });
      }
    })
      .then(async result => {
        await options.task[task.type]({ task, result });
        await task.update({
          status: 'success',
          output: result,
          progress: 100,
          completedAt: new Date()
        });
      })
      .catch(error => {
        return task.update({
          status: 'failed',
          error: error.toString(),
          completedAt: new Date()
        });
      });
  };

  const runner = async () => {
    const pendingTasks = await models.task.findAll({
      where: {
        runnerType: 'system',
        status: 'pending',
        startTime: {
          [Op.lte]: new Date()
        }
      },
      limit: options.limit
    });

    const count = await models.task.count({
      where: {
        runnerType: 'system',
        status: 'pending'
      }
    });

    if (pendingTasks.length > 0) {
      fastify.log.info(`本轮执行 ${pendingTasks.length}/${count} 个待处理的系统任务`);
      await Promise.all(pendingTasks.map(async task => processSystemTask(task)));
    }
  };

  const detail = async ({ id }) => {
    const task = await models.task.findByPk(id);
    if (!task) {
      throw new Error('任务不存在');
    }
    return task;
  };

  const cancel = async ({ id, targetId, targetType }) => {
    if (targetId && targetType) {
      return await models.task.update(
        {
          status: 'canceled'
        },
        {
          where: {
            targetId,
            targetType,
            status: {
              [Op.in]: ['pending', 'running']
            }
          }
        }
      );
    }
    if (id) {
      const task = await detail({ id });
      return await task.update({
        status: 'canceled'
      });
    }
    throw new Error('参数错误');
  };

  const complete = async ({ id, userId, output, ...props }) => {
    const task = await detail({ id });
    if (props.status === 'success') {
      await options.task[task.type]({ task, result: output });
      await task.update({
        status: 'success',
        output,
        progress: 100,
        completedAt: new Date(),
        completedUserId: userId
      });
    } else {
      await task.update(
        Object.assign({}, props, {
          status: 'failed',
          completedAt: new Date(),
          completedUserId: userId
        })
      );
    }
  };

  const list = async ({ filter, perPage, currentPage }) => {
    const whereQuery = {};

    ['targetId', 'type', 'status', 'runnerType'].forEach(key => {
      if (filter && filter[key]) {
        whereQuery[key] = filter[key];
      }
    });

    const { rows, count } = await models.task.findAndCountAll({
      where: Object.assign({}, whereQuery),
      offset: perPage * (currentPage - 1),
      limit: perPage,
      order: [['createdAt', 'DESC']]
    });

    return {
      pageData: rows,
      totalCount: count
    };
  };

  const retry = async ({ id }) => {
    const task = await detail({ id });
    if (task.status !== 'failed') {
      throw new Error('只有失败的任务允许重试');
    }
    await task.update({
      status: 'pending',
      completedAt: null,
      completedUserId: null
    });
  };

  Object.assign(fastify[options.name].services, {
    create,
    detail,
    list,
    complete,
    cancel,
    runner,
    resetAll,
    retry
  });
});

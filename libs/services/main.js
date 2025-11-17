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

  const executor = async ({ type, scriptName, ...props }) => {
    const taskModulePath = path.resolve(options.dir, type, `${scriptName || options.scriptName}.js`);
    if (!(await fs.exists(taskModulePath))) {
      throw new Error(`未匹配到任务执行器:${taskModulePath}`);
    }
    return await require(taskModulePath)(fastify, options, {
      ...props,
      updateProgress: async progress => {
        typeof props?.task?.update === 'function' &&
          (await props.task.update({
            progress: progress
          }));
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
              reject(new Error(`轮询超时（${maxPollTimes}次），任务未完成`));
            }
            try {
              const pollingResult = Object.assign({}, await callback());
              const { result, data, message, progress } = pollingResult;
              if (typeof props?.task?.update === 'function') {
                await props.task.reload();
                await props.task.update(
                  Object.assign(
                    {},
                    {
                      pollCount: props.task.pollCount + 1,
                      pollResults: [...props.task.pollResults, Object.assign({}, pollingResult, { time: new Date() })]
                    },
                    Number.isInteger(progress) ? { progress } : {}
                  )
                );
              }
              if (result === 'failed') {
                clearInterval(timer);
                reject(new Error(`任务处理失败:${message}`));
              }
              if (result === 'success') {
                clearInterval(timer);
                resolve(data);
              }
            } catch (e) {
              reject(e);
            }
          }, pollInterval);
        });
      },
      next: async context => {
        if (typeof props?.task?.update === 'function') {
          await props.task.update({ context, status: 'waiting' });
        }
        return false;
      }
    });
  };

  const processNext = async ({ id, signature, result: resultStr }) => {
    const task = await detail({ id });
    if (task.status !== 'waiting') {
      throw new Error('当前任务状态不允许执行Next操作');
    }
    if (task.context?.secret) {
      const dataToSign = `${id}|${resultStr}`;
      // 使用 HMAC-SHA256 生成签名
      const hmac = crypto.createHmac('sha256', task.context.secret);
      hmac.update(dataToSign);
      if (signature !== hmac.digest('hex')) {
        throw new Error('签名验证失败');
      }
    }
    const result = JSON.parse(resultStr);
    if (result.code !== 0) {
      await task.update({
        status: 'failed',
        error: result,
        completedAt: new Date()
      });
      return;
    }
    await options.task[task.type]({ task, result: result.data, context: task.context });
    await task.update({
      status: 'success',
      output: result,
      progress: 100,
      completedAt: new Date()
    });
  };

  const processSystemTask = async task => {
    try {
      await task.update({
        status: 'running',
        progress: 0,
        error: null,
        output: null,
        pollCount: 0,
        pollResults: [],
        completedAt: null,
        context: {}
      });
      executor({ type: task.type, scriptName: task.scriptName, task })
        .then(async result => {
          if (result === false) {
            return;
          }
          await options.task[task.type]({ task, result, context: task.context });
          await task.update({
            status: 'success',
            output: result,
            progress: 100,
            completedAt: new Date()
          });
        })
        .catch(e => {
          return task.update({
            status: 'failed',
            error: (e.stack || '').replaceAll(process.cwd(), '/server'),
            completedAt: new Date()
          });
        });
    } catch (e) {
      await task.update({
        status: 'failed',
        error: (e.stack || '').replaceAll(process.cwd(), '/server'),
        completedAt: new Date()
      });
    }
  };

  const runner = async () => {
    const runningTaskCount = await models.task.count({
      where: {
        runnerType: 'system',
        status: 'running'
      }
    });
    const limit = options.limit - runningTaskCount;
    if (limit <= 0) {
      fastify.log.info(`当前运行中的系统任务数(${runningTaskCount})已达上限(${options.limit})，暂不执行新任务`);
      return;
    }
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

  const cancel = async ({ id, targetId, targetType, type }) => {
    if (targetId && targetType && type) {
      return await models.task.update(
        {
          status: 'canceled'
        },
        {
          where: {
            targetId,
            targetType,
            type,
            status: {
              [Op.in]: ['pending', 'running']
            }
          }
        }
      );
    }
    if (id) {
      const task = await detail({ id });
      if (!['pending', 'running'].includes(task.status)) {
        return;
      }
      return await task.update({
        status: 'canceled'
      });
    }
  };

  const complete = async ({ id, userId, output, ...props }) => {
    const task = await detail({ id });
    if (!task) {
      throw new Error('任务不存在');
    }
    if (props.status === 'success') {
      try {
        await options.task[task.type]({ task, result: output });
        await task.update({
          status: 'success',
          output,
          progress: 100,
          completedAt: new Date(),
          completedUserId: userId
        });
      } catch (e) {
        await task.update(
          Object.assign({}, props, {
            status: 'failed',
            output,
            error: (e.stack || '').replaceAll(process.cwd(), '/server'),
            completedAt: new Date()
          })
        );
        throw e;
      }
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

  const getTimeQuery = fieldValue => {
    const { startTime, endTime } = fieldValue;
    if (!!startTime && !!endTime) {
      // 两个日期都有值，使用between
      return {
        [Op.between]: [new Date(startTime), new Date(endTime)]
      };
    } else if (!!startTime) {
      // 只有开始日期，大于等于
      return {
        [Op.gte]: new Date(startTime)
      };
    } else if (!!endTime) {
      // 只有结束日期，小于等于
      return {
        [Op.lte]: new Date(endTime)
      };
    }
  };

  const list = async ({ filter, perPage, currentPage, sort }) => {
    const whereQuery = {};

    ['id', 'targetId', 'type', 'status', 'runnerType'].forEach(key => {
      if (filter && filter[key]) {
        whereQuery[key] = filter[key];
      }
    });

    // 处理targetName模糊匹配，查询input.name字段
    if (filter && filter.targetName) {
      whereQuery['input.name'] = {
        [Op.like]: `%${filter.targetName}%`
      };
    }

    // 处理createdAt日期范围查询
    if (filter && filter.createdAt) {
      whereQuery.createdAt = getTimeQuery(filter.createdAt);
    }

    // 处理completedAt日期范围查询
    if (filter && filter.completedAt) {
      whereQuery.completedAt = getTimeQuery(filter.completedAt);
    }

    // 处理sort排序
    let orderBy = 'createdAt';
    let orderDirection = 'DESC';

    if (sort && Object.keys(sort).length > 0) {
      Object.keys(sort).forEach(key => {
        orderBy = key;
        orderDirection = sort[key];
      });
    }

    const { rows, count } = await models.task.findAndCountAll({
      where: Object.assign({}, whereQuery),
      offset: perPage * (currentPage - 1),
      limit: perPage,
      order: [[orderBy, orderDirection]]
    });

    return {
      pageData: rows,
      totalCount: count
    };
  };

  const retryFunc = async ({ id }) => {
    const task = await detail({ id });
    if (['failed', 'canceled'].indexOf(task.status) === -1) {
      throw new Error('只有失败或取消的任务允许重试');
    }
    await task.update({
      status: 'pending',
      completedAt: null,
      completedUserId: null
    });
  };

  const retry = async ({ id, taskIds }) => {
    if (id) {
      await retryFunc({ id });
    }
    if (taskIds && taskIds.length > 0) {
      for (const taskId of taskIds) {
        await retryFunc({ id: taskId });
      }
    }
  };

  Object.assign(fastify[options.name].services, {
    create,
    detail,
    list,
    complete,
    cancel,
    runner,
    resetAll,
    retry,
    executor,
    processNext,
    processSystemTask
  });
});

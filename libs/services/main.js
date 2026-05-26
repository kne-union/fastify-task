const fp = require('fastify-plugin');
const path = require('node:path');
const fs = require('fs-extra');
const crypto = require('node:crypto');

module.exports = fp(async (fastify, options) => {
  const { models } = fastify[options.name];
  const { Op } = fastify.sequelize.Sequelize;

  /** 通过 @kne/fastify-statistics 采集任务完成数据 */
  const collectTaskStatistics = async task => {
    // 确保 task 数据是最新的（Sequelize update 后实例可能未完全同步）
    if (task.reload && typeof task.reload === 'function') {
      await task.reload();
    }
    const completedAt = task.completedAt || new Date();
    const createdAt = task.createdAt;
    const startedAt = task.startedAt;
    let waitingTime = 0;
    let executionTime = 0;
    let totalTime = 0;
    let hasTiming = false;

    if (createdAt && completedAt) {
      totalTime = new Date(completedAt).getTime() - new Date(createdAt).getTime();
      if (startedAt) {
        waitingTime = new Date(startedAt).getTime() - new Date(createdAt).getTime();
        executionTime = new Date(completedAt).getTime() - new Date(startedAt).getTime();
      } else {
        executionTime = totalTime;
      }
      hasTiming = totalTime > 0;
    }

    const channel = `task:${task.type}:${task.runnerType || 'manual'}`;
    const status = task.status || 'unknown';

    // 分两次采集：计数指标和时长指标，避免聚合时不同语义的值混在一起
    const countData = { total: 1, [status]: 1 };
    fastify[`${options.name}Statistics`].services
      .collect({
        channel,
        data: countData,
        unit: 'count',
        time: completedAt
      })
      .catch(e => {
        fastify.log.error(`采集任务统计数据失败: ${e.message}`);
      });

    if (hasTiming) {
      const timingData = { waitingTime, executionTime, totalTime };
      fastify[`${options.name}Statistics`].services
        .collect({
          channel,
          data: timingData,
          unit: 'ms',
          time: completedAt
        })
        .catch(e => {
          fastify.log.error(`采集任务统计数据失败: ${e.message}`);
        });
    }
  };

  const generateSignature = ({ secret, id, data }) => {
    const dataToSign = `${id}|${typeof data === 'string' ? data : JSON.stringify(data)}`;
    const hmac = crypto.createHmac('sha256', secret);
    hmac.update(dataToSign);
    return hmac.digest('hex');
  };

  const verifySignature = ({ secret, id, data, signature }) => {
    if (!secret) return true;
    const expectedSignature = generateSignature({ secret, id, data });
    return signature === expectedSignature;
  };

  const create = async ({ userId, input, type, targetId, targetType, runnerType, delay = 0, scriptName, priority = 0, parentTaskId, maxRetries = 0, options: currentOptions }) => {
    if (typeof delay !== 'number' || delay < 0) {
      throw new Error('delay 必须为非负数');
    }
    if (typeof priority !== 'number' || !Number.isInteger(priority)) {
      throw new Error('priority 必须为整数');
    }
    if (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new Error('maxRetries 必须为非负整数');
    }
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
      priority,
      parentTaskId,
      maxRetries,
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
    const scriptFile = `${scriptName || options.scriptName}.js`;
    let taskModulePath = null;
    for (const dir of options.dirs) {
      const candidate = path.resolve(dir, type, scriptFile);
      if (await fs.exists(candidate)) {
        taskModulePath = candidate;
        break;
      }
    }
    if (!taskModulePath) {
      throw new Error(`未匹配到任务执行器:${type}/${scriptFile}，已搜索目录:${options.dirs.join(',')}`);
    }
    // 清除 require 缓存，确保运行期间模块更新后能加载最新版本
    delete require.cache[taskModulePath];
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
          const executePolling = async () => {
            try {
              pollCount++;
              if (pollCount > maxPollTimes) {
                reject(new Error(`轮询超时（${maxPollTimes}次），任务未完成`));
                return;
              }

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
                reject(new Error(`任务处理失败:${message}`));
                return;
              }

              if (result === 'success') {
                resolve(data);
                return;
              }

              // 任务未完成，等待pollInterval后继续执行
              setTimeout(executePolling, pollInterval);
            } catch (e) {
              reject(e);
            }
          };
          // 开始第一次轮询
          setTimeout(executePolling, pollInterval);
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
    if (task.context?.secret && !verifySignature({ secret: task.context.secret, id, data: resultStr, signature })) {
      throw new Error('签名验证失败');
    }
    const result = JSON.parse(resultStr);
    if (result.code !== 0) {
      await task.update({
        status: 'failed',
        error: result,
        completedAt: new Date()
      });
      collectTaskStatistics(task);
      return;
    }
    await options.task[task.type]({ task, result: result.data, context: task.context });
    await task.update({
      status: 'success',
      output: result,
      progress: 100,
      completedAt: new Date()
    });
    collectTaskStatistics(task);
  };

  const processSystemTask = async task => {
    await task.update({
      status: 'running',
      startedAt: new Date(),
      progress: 0,
      error: null,
      output: null,
      pollCount: 0,
      pollResults: [],
      completedAt: null,
      context: {}
    });
    const addLog = props => {
      return log(Object.assign({}, props, { taskId: task.id }));
    };

    // #3: 任务超时自动失败
    const createTimeout = ms => new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`任务执行超时(${ms}ms)`)), ms)
    );

    const executorPromise = executor({ type: task.type, scriptName: task.scriptName, task, log: addLog });
    const racePromise = options.taskTimeout > 0
      ? Promise.race([executorPromise, createTimeout(options.taskTimeout)])
      : executorPromise;

    return racePromise
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
        collectTaskStatistics(task);
        // #2: 任务依赖/链式执行 — 父任务成功后激活子任务
        await triggerChildTasks(task);
      })
      .catch(async e => {
        // #4: 重试策略 — 判断是否自动重试
        const currentRetryCount = (task.retryCount || 0) + 1;
        const maxRetries = task.maxRetries || 0;
        if (currentRetryCount < maxRetries) {
          // 指数退避：baseDelay * 2^retryCount
          const baseDelay = options.retryBaseDelay || 5000;
          const backoffDelay = baseDelay * Math.pow(2, currentRetryCount - 1);
          await task.update({
            status: 'pending',
            retryCount: currentRetryCount,
            error: (e.stack || '').replaceAll(process.cwd(), '/server'),
            startTime: new Date(Date.now() + backoffDelay),
            completedAt: null
          });
          fastify.log.info(`任务 ${task.id} 第 ${currentRetryCount} 次重试，${backoffDelay}ms 后执行`);
        } else {
          await task.update({
            status: 'failed',
            error: (e.stack || '').replaceAll(process.cwd(), '/server'),
            completedAt: new Date(),
            retryCount: currentRetryCount
          });
          collectTaskStatistics(task);
        }
      });
  };

  // #2: 触发子任务（父任务成功后激活 pending 的子任务）
  const triggerChildTasks = async parentTask => {
    const childTasks = await models.task.findAll({
      where: {
        parentTaskId: parentTask.id,
        status: 'pending'
      }
    });
    if (childTasks.length === 0) return;

    fastify.log.info(`父任务 ${parentTask.id} 完成，激活 ${childTasks.length} 个子任务`);
    for (const child of childTasks) {
      if (child.runnerType === 'system') {
        await processSystemTask(child);
      }
      // manual 类型子任务保持 pending，等待手动执行
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
    // #1: 按优先级降序 + startTime 升序获取待处理任务
    const pendingTasks = await models.task.findAll({
      where: {
        runnerType: 'system',
        status: 'pending',
        startTime: {
          [Op.lte]: new Date()
        }
      },
      order: [
        ['priority', 'DESC'],
        ['startTime', 'ASC']
      ],
      limit: limit
    });

    if (pendingTasks.length > 0) {
      fastify.log.info(`本轮执行 ${pendingTasks.length} 个待处理的系统任务`);
      // #8: await processSystemTask 返回的 Promise，防止同任务重复执行
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
    if (!id && !(targetId && targetType && type)) {
      throw new Error('必须提供 id 或 targetId+targetType+type');
    }
    if (targetId && targetType && type) {
      const bulkWhere = {
        targetId,
        targetType,
        type,
        status: {
          [Op.in]: ['pending', 'running']
        }
      };
      const tasksToCancel = await models.task.findAll({ where: bulkWhere, raw: true });
      const completedAt = new Date();
      const [affectedCount] = await models.task.update(
        {
          status: 'canceled',
          completedAt
        },
        {
          where: bulkWhere
        }
      );
      for (const row of tasksToCancel) {
        collectTaskStatistics(Object.assign({}, row, { status: 'canceled', completedAt }));
      }
      return affectedCount;
    }
    if (id) {
      const task = await detail({ id });
      if (!['pending', 'running'].includes(task.status)) {
        return;
      }
      await task.update({
        status: 'canceled',
        completedAt: new Date()
      });
      collectTaskStatistics(task);
    }
  };

  const complete = async ({ id, userId, output, ...props }) => {
    const task = await detail({ id });
    if (props.status === 'success') {
      try {
        await options.task[task.type]({ task, result: output });
        await task.update({
          status: 'success',
          output,
          progress: 100,
          completedAt: new Date(),
          startedAt: task.startedAt || task.createdAt,
          completedUserId: userId
        });
        collectTaskStatistics(task);
      } catch (e) {
        await task.update(
          Object.assign({}, props, {
            status: 'failed',
            output,
            error: (e.stack || '').replaceAll(process.cwd(), '/server'),
            completedAt: new Date(),
            startedAt: task.startedAt || task.createdAt
          })
        );
        collectTaskStatistics(task);
        throw e;
      }
    } else {
      await task.update(
        Object.assign({}, props, {
          status: 'failed',
          completedAt: new Date(),
          startedAt: task.startedAt || task.createdAt,
          completedUserId: userId
        })
      );
      collectTaskStatistics(task);
    }
  };

  const waitingComplete = async ({ id, pollInterval = 1000, maxPollTimes = 20 }) => {
    const task = await detail({ id });

    if (task.status === 'pending') {
      // 标记为最高优先级，确保立即执行（即使因重试等重新入队也会被优先处理）
      await task.update({ priority: Number.MAX_SAFE_INTEGER });
      await processSystemTask(task);
    }

    let pollCount = 0;
    return await new Promise((resolve, reject) => {
      const executePolling = async () => {
        try {
          pollCount++;
          if (pollCount > maxPollTimes) {
            reject(new Error('任务超时'));
            return;
          }

          await task.reload();
          const { status, output, error } = task;

          if (status === 'success') {
            resolve(output);
            return;
          }

          if (status === 'failed' || status === 'canceled') {
            reject(new Error(error || `任务${status === 'failed' ? '失败' : '取消'}`));
            return;
          }

          // 任务未完成，等待pollInterval后继续执行
          setTimeout(executePolling, pollInterval);
        } catch (e) {
          reject(e);
        }
      };
      // 开始第一次轮询
      setTimeout(executePolling, pollInterval);
    });
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

  const list = async ({ filter, perPage = 20, currentPage = 1, sort }) => {
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

    // 处理sort排序：支持多字段排序
    let order = [['createdAt', 'DESC']];

    if (sort && Object.keys(sort).length > 0) {
      order = Object.entries(sort).map(([key, direction]) => [key, direction]);
    }

    const { rows, count } = await models.task.findAndCountAll({
      where: Object.assign({}, whereQuery),
      offset: perPage * (currentPage - 1),
      limit: perPage,
      order: order
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
      completedUserId: null,
      retryCount: 0,
      error: null
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

  const log = async ({ id, taskId, data, message = '' }) => {
    const targetId = id || taskId;
    const task = await detail({ id: targetId });

    const currentOptions = task.options || {};
    const logs = (currentOptions.logs || []).slice(0);
    logs.splice(0, 0, {
      data,
      message,
      time: new Date()
    });
    if (logs.length > 100) {
      logs.splice(0, logs.length - 100);
    }
    await task.update({
      options: {
        ...currentOptions,
        logs
      }
    });

    return task;
  };

  const logWithSignature = async ({ id, taskId, data, message = '', signature }) => {
    const targetId = id || taskId;
    const task = await detail({ id: targetId });

    if (task.context?.secret && !verifySignature({ secret: task.context.secret, id: targetId, data: { data, message }, signature })) {
      throw new Error('签名验证失败');
    }

    return log({ id: targetId, data, message });
  };

  const callback = async ({ id, code, data, message }) => {
    await detail({ id });

    const result = { code, data, message };
    await log({ id, message: '回调结果', data: JSON.stringify(result) });
    const input = Object.assign(
      {},
      { id },
      code === 0
        ? {
            status: 'success',
            output: data
          }
        : {
            status: 'failed',
            output: data,
            error: message
          }
    );
    await complete(input);
  };

  const callbackWithSignature = async ({ id, code, data, message, signature }) => {
    const task = await detail({ id });

    if (task.context?.secret && !verifySignature({ secret: task.context.secret, id, data: { code, data, message }, signature })) {
      throw new Error('签名验证失败');
    }

    return callback({ id, code, data, message });
  };

  const append = async ({ dirs, tasks }) => {
    const result = { dirs: [], tasks: [] };
    if (Array.isArray(dirs)) {
      for (const dir of dirs) {
        if (!options.dirs.includes(dir)) {
          if (!(await fs.exists(dir))) {
            console.warn(`append 目录不存在:${dir}，仍会添加但运行时可能无法匹配任务执行器`);
          }
          options.dirs.push(dir);
          result.dirs.push(dir);
        }
      }
    }
    if (tasks && typeof tasks === 'object') {
      Object.entries(tasks).forEach(([type, handler]) => {
        if (typeof handler !== 'function') {
          throw new Error(`任务 ${type} 的 handler 必须是一个函数`);
        }
        if (typeof options.task[type] !== 'function') {
          options.task[type] = handler;
          result.tasks.push(type);
        }
      });
    }
    return result;
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
    log,
    logWithSignature,
    callback,
    callbackWithSignature,
    executor,
    processNext,
    processSystemTask,
    waitingComplete,
    append
  });
});

const fp = require('fastify-plugin');
const fs = require('fs-extra');
const SecurityValidator = require('../utils/security-validator');
const {
  TaskNotFoundError,
  TaskStatusError,
  TaskValidationError,
  TaskExecutionError
} = require('../utils/errors');
const {
  CONFIG_CONSTANTS,
  getConfigManager
} = require('../utils/config');
const {
  getCacheManager
} = require('../utils/cache-manager');
const TaskExecutor = require('../utils/modules/task-executor');

module.exports = fp(async (fastify, options) => {
  const { models } = fastify[options.name];
  const { Op } = fastify.sequelize.Sequelize;

  // 确保模块能够正确访问 SecurityValidator
  const securityValidator = SecurityValidator;

  // 初始化配置管理器
  const configManager = getConfigManager(options);
  const config = configManager.getAll();

  // 初始化缓存管理器
  const cacheManager = getCacheManager(300000); // 5分钟缓存

  // 初始化模块化组件
  const taskExecutor = new TaskExecutor(fastify, options, models, securityValidator, cacheManager);

  // 统一调用 errorHandler
  const handleError = async ({ task, error, type }) => {
    if (typeof options.errorHandler === 'function') {
      try {
        await options.errorHandler({ task, error, type });
      } catch (handlerError) {
        fastify.log.error(`errorHandler 执行失败: ${handlerError.message}`);
      }
    }
  };

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

    const completedHour = new Date(completedAt).getHours();
    const channel = `${task.type}:${task.runnerType || CONFIG_CONSTANTS.RUNNER_TYPES.MANUAL}:${completedHour}`;
    const status = task.status || 'unknown';

    const data = { total: 1, [status]: 1 };
    const unit = { total: 'count', [status]: 'count' };
    if (hasTiming) {
      Object.assign(data, { waitingTime, executionTime, totalTime });
      Object.assign(unit, { waitingTime: 'ms', executionTime: 'ms', totalTime: 'ms' });
    }

    try {
      await fastify[`${options.name}Statistics`].services.collect({
        channel,
        data,
        unit,
        time: completedAt
      });
    } catch (error) {
      fastify.log.error(`采集任务统计数据失败: ${error.message}`, { taskId: task.id, error: error.stack });
      // 不抛出错误，避免影响主流程
    }
  };

  const create = async ({ userId, input, type, targetId, targetType, runnerType, delay = 0, scriptName, priority = 0, parentTaskId, maxRetries = 0, timeout = 60, options: currentOptions }) => {
    try {
      // 参数验证
      delay = SecurityValidator.validateNumber(delay, {
        min: CONFIG_CONSTANTS.DELAY_MIN,
        max: CONFIG_CONSTANTS.DELAY_MAX,
        integer: true,
        required: false
      });

      priority = SecurityValidator.validateNumber(priority, {
        min: CONFIG_CONSTANTS.PRIORITY_MIN,
        max: CONFIG_CONSTANTS.PRIORITY_MAX,
        integer: true
      });

      maxRetries = SecurityValidator.validateNumber(maxRetries, {
        min: 0,
        max: CONFIG_CONSTANTS.RETRY_MAX_COUNT,
        integer: true
      });

      timeout = SecurityValidator.validateNumber(timeout, {
        min: CONFIG_CONSTANTS.TASK_TIMEOUT_MIN,
        max: CONFIG_CONSTANTS.TASK_TIMEOUT_MAX / (60 * 1000), // 转换为分钟
        integer: true
      });

      // 任务类型验证
      SecurityValidator.validateTaskType(type, Object.keys(options.task || {}));

      // 检查任务处理器是否存在
      if (typeof options.task[type] !== 'function') {
        throw new TaskValidationError('type', type, '未找到合法的任务声明');
      }

      const task = await models.task.create({
        userId,
        input,
        type,
        targetId,
        targetType,
        runnerType,
        priority,
        parentTaskId,
        maxRetries,
        timeout,
        startTime: delay > 0 ? new Date(Date.now() + 1000 * delay) : new Date(),
        scriptName,
        options: currentOptions
      });

      return task;
    } catch (error) {
      if (error instanceof TaskValidationError) {
        throw error;
      }
      throw new TaskExecutionError(null, error, { operation: 'create' });
    }
  };

  const resetAll = async () => {
    const runningTasks = await models.task.findAll({
      where: { status: 'running' },
      attributes: ['type', 'runnerType'],
      raw: true
    });
    await models.task.update(
      { status: 'pending' },
      { where: { status: 'running' } }
    );
  };

  const executor = async ({ type, scriptName, ...props }) => {
    return await taskExecutor.execute({ type, scriptName, ...props });
  };

  const processNext = async ({ id, signature, result: resultStr }) => {
    try {
      const task = await detail({ id });

      if (task.status !== CONFIG_CONSTANTS.TASK_STATUSES.WAITING) {
        throw new TaskStatusError(id, task.status, CONFIG_CONSTANTS.TASK_STATUSES.WAITING);
      }

      // 签名验证
      if (task.context?.secret && config.security.enableSignature) {
        if (!SecurityValidator.verifySignature({
          secret: task.context.secret,
          id,
          data: resultStr,
          signature
        })) {
          throw new TaskValidationError('signature', signature, '签名验证失败');
        }
      }

      const result = SecurityValidator.safeJSONParse(resultStr);
      if (!result || typeof result !== 'object') {
        throw new TaskValidationError('result', resultStr, '结果必须是有效的JSON对象');
      }

      if (result.code !== 0) {
        await task.update({
          status: CONFIG_CONSTANTS.TASK_STATUSES.FAILED,
          error: result,
          completedAt: new Date()
        });
        collectTaskStatistics(task);
        await handleError({ task, error: result, type: 'callback' });
        return;
      }

      await config.task[task.type]({ task, result: result.data, context: task.context });
      await task.update({
        status: CONFIG_CONSTANTS.TASK_STATUSES.SUCCESS,
        output: result,
        progress: 100,
        completedAt: new Date()
      });
      collectTaskStatistics(task);
    } catch (error) {
      if (error instanceof TaskStatusError || error instanceof TaskValidationError) {
        throw error;
      }
      throw new TaskExecutionError(id, error, { operation: 'processNext' });
    }
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
          await handleError({ task, error: e.message || e, type: currentRetryCount > 1 ? 'retry_exhausted' : 'execution' });
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

  /** 检查超时任务，将超时的 running/waiting 任务标记为 failed */
  const checkTimeout = async () => {
    const now = new Date();
    const timedOutTasks = await models.task.findAll({
      where: {
        status: { [Op.in]: ['running', 'waiting'] },
        timeout: { [Op.gt]: 0 },
        startedAt: { [Op.ne]: null }
      }
    });

    const tasksToFail = timedOutTasks.filter(task => {
      const elapsed = now.getTime() - new Date(task.startedAt).getTime();
      return elapsed > task.timeout * 60 * 1000;
    });

    if (tasksToFail.length === 0) return;

    fastify.log.info(`检测到 ${tasksToFail.length} 个超时任务`);

    for (const task of tasksToFail) {
      const elapsed = now.getTime() - new Date(task.startedAt).getTime();
      const prevStatus = task.status;
      await task.update({
        status: 'failed',
        error: `任务超时：已执行 ${Math.round(elapsed / 60000)} 分钟，超过设定的 ${task.timeout} 分钟超时时间`,
        completedAt: now
      });
      collectTaskStatistics(task);
      await handleError({ task, error: task.error, type: 'timeout' });
    }
  };

  const runner = async () => {
    // 检查超时任务
    await checkTimeout();

    // 批量查询运行中和待处理的任务，减少数据库往返
    const [runningTasks, pendingTasks] = await Promise.all([
      models.task.count({
        where: {
          runnerType: CONFIG_CONSTANTS.RUNNER_TYPES.SYSTEM,
          status: CONFIG_CONSTANTS.TASK_STATUSES.RUNNING
        }
      }),
      models.task.findAll({
        where: {
          runnerType: CONFIG_CONSTANTS.RUNNER_TYPES.SYSTEM,
          status: CONFIG_CONSTANTS.TASK_STATUSES.PENDING,
          startTime: {
            [Op.lte]: new Date()
          }
        },
        order: [
          ['priority', 'DESC'],
          ['startTime', 'ASC']
        ],
        limit: config.limit
      })
    ]);

    const runningTaskCount = runningTasks;
    const limit = config.limit - runningTaskCount;

    if (limit <= 0) {
      fastify.log.info(`当前运行中的系统任务数(${runningTaskCount})已达上限(${config.limit})，暂不执行新任务`);
      return;
    }

    // 取出可以执行的任务
    const tasksToExecute = pendingTasks.slice(0, limit);

    if (tasksToExecute.length > 0) {
      fastify.log.info(`本轮执行 ${tasksToExecute.length} 个待处理的系统任务`);

      // 批量更新任务状态为 running，防止重复执行
      const taskIds = tasksToExecute.map(task => task.id);
      await models.task.update(
        { status: CONFIG_CONSTANTS.TASK_STATUSES.RUNNING, startedAt: new Date() },
        { where: { id: { [Op.in]: taskIds } } }
      );

      // 清除相关缓存
      taskIds.forEach(id => cacheManager.delete(`task:${id}`));

      // 并行执行任务
      await Promise.all(tasksToExecute.map(async task => {
        try {
          await processSystemTask(task);
        } catch (error) {
          fastify.log.error(`任务执行失败: ${task.id}`, { error: error.message });
        }
      }));
    }
  };

  const detail = async ({ id }) => {
    if (!id) {
      throw new TaskValidationError('id', id, '任务ID不能为空');
    }

    // 使用缓存优化查询
    const cacheKey = `task:${id}`;
    const cachedTask = cacheManager.get(cacheKey);

    if (cachedTask) {
      return cachedTask;
    }

    const task = await models.task.findByPk(id);
    if (!task) {
      throw new TaskNotFoundError(id);
    }

    // 缓存任务数据（短时间缓存，因为状态会变化）
    cacheManager.set(cacheKey, task, 60000); // 1分钟缓存

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
    const prevStatus = task.status;
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
        await handleError({ task, error: e.message || e, type: 'execution' });
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
      await handleError({ task, error: props.error || '任务标记为失败', type: 'callback' });
    }
  };

  const waitingComplete = async ({ id, pollInterval = 1000, maxPollTimes = 20 }) => {
    let task = await detail({ id });

    if (task.status === 'pending') {
      // 标记为最高优先级，确保立即执行（即使因重试等重新入队也会被优先处理）
      await task.update({ priority: Number.MAX_SAFE_INTEGER });
      await processSystemTask(task);
      // processSystemTask 完成后重新加载任务状态
      await task.reload();
    }

    // 立即检查是否已到达终态
    if (task.status === 'success') {
      return task.output;
    }
    if (task.status === 'failed') {
      throw new Error(task.error || '任务失败');
    }
    if (task.status === 'canceled') {
      throw new Error(task.error || '任务取消');
    }

    // 任务仍在运行中，开始轮询
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
    try {
      const targetId = id || taskId;
      const task = await detail({ id: targetId });

      const currentOptions = task.options || {};
      const logs = currentOptions.logs || [];

      // 性能优化：只在日志超过限制时才进行截断
      const newLog = { data, message, time: new Date() };
      let limitedLogs;

      if (logs.length >= CONFIG_CONSTANTS.LOGS_MAX_SIZE) {
        // 使用更高效的数组操作
        limitedLogs = [...logs.slice(-CONFIG_CONSTANTS.LOGS_MAX_SIZE + 1), newLog];
      } else {
        limitedLogs = [...logs, newLog];
      }

      await task.update({
        options: {
          ...currentOptions,
          logs: limitedLogs
        }
      });

      // 更新缓存
      cacheManager.set(`task:${targetId}`, task, 60000);

      return task;
    } catch (error) {
      if (error instanceof TaskNotFoundError || error instanceof TaskValidationError) {
        throw error;
      }
      throw new TaskExecutionError(id || taskId, error, { operation: 'log' });
    }
  };

  const logWithSignature = async ({ id, taskId, data, message = '', signature }) => {
    try {
      const targetId = id || taskId;
      const task = await detail({ id: targetId });

      // 签名验证
      if (task.context?.secret && config.security.enableSignature) {
        if (!SecurityValidator.verifySignature({
          secret: task.context.secret,
          id: targetId,
          data: { data, message },
          signature
        })) {
          throw new TaskValidationError('signature', signature, '签名验证失败');
        }
      }

      return log({ id: targetId, data, message });
    } catch (error) {
      if (error instanceof TaskValidationError) {
        throw error;
      }
      throw new TaskExecutionError(id || taskId, error, { operation: 'logWithSignature' });
    }
  };

  const callback = async ({ id, code, data, message }) => {
    try {
      // 参数验证
      const validatedCode = SecurityValidator.validateNumber(code, {
        min: 0,
        max: Number.MAX_SAFE_INTEGER,
        integer: true
      });

      await detail({ id });

      const result = { code: validatedCode, data, message };
      await log({ id, message: '回调结果', data: JSON.stringify(result) });

      const input = Object.assign(
        {},
        { id },
        validatedCode === 0
          ? {
              status: CONFIG_CONSTANTS.TASK_STATUSES.SUCCESS,
              output: data
            }
          : {
              status: CONFIG_CONSTANTS.TASK_STATUSES.FAILED,
              output: data,
              error: message
            }
      );
      await complete(input);
    } catch (error) {
      if (error instanceof TaskNotFoundError || error instanceof TaskValidationError) {
        throw error;
      }
      throw new TaskExecutionError(id, error, { operation: 'callback' });
    }
  };

  const callbackWithSignature = async ({ id, code, data, message, signature }) => {
    try {
      const task = await detail({ id });

      // 签名验证
      if (task.context?.secret && config.security.enableSignature) {
        if (!SecurityValidator.verifySignature({
          secret: task.context.secret,
          id,
          data: { code, data, message },
          signature
        })) {
          throw new TaskValidationError('signature', signature, '签名验证失败');
        }
      }

      return callback({ id, code, data, message });
    } catch (error) {
      if (error instanceof TaskValidationError) {
        throw error;
      }
      throw new TaskExecutionError(id, error, { operation: 'callbackWithSignature' });
    }
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

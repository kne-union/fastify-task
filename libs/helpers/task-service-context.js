const path = require('node:path');
const fs = require('fs-extra');
const crypto = require('node:crypto');

const CONTEXT_KEY = Symbol.for('@kne/fastify-task.serviceContext');

const createContext = (fastify, options) => {
  const { models } = fastify[options.name];
  const { Op } = fastify.sequelize.Sequelize;
  const mainNamespace = options.name;
  options.taskRegistry = options.taskRegistry || {};

  const collectTaskStatistics = async task => {
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
    const channel = `${task.type}:${task.runnerType || 'manual'}:${completedHour}`;
    const status = task.status || 'unknown';

    const data = { total: 1, [status]: 1 };
    const unit = { total: 'count', [status]: 'count' };
    if (hasTiming) {
      Object.assign(data, { waitingTime, executionTime, totalTime });
      Object.assign(unit, { waitingTime: 'ms', executionTime: 'ms', totalTime: 'ms' });
    }
    fastify[`${options.name}Statistics`].services
      .collect({
        channel,
        data,
        unit,
        time: completedAt
      })
      .catch(e => {
        fastify.log.error(`采集任务统计数据失败: ${e.message}`);
      });
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

  const normalizeDirs = ({ dir, dirs } = {}) => {
    const list = [];
    if (dir) list.push(dir);
    if (Array.isArray(dirs)) list.push(...dirs);
    return [...new Set(list.filter(Boolean))];
  };

  const splitTaskName = type => {
    const index = String(type || '').indexOf('.');
    if (index === -1) {
      return { namespace: null, taskName: type };
    }
    return {
      namespace: String(type).slice(0, index),
      taskName: String(type).slice(index + 1)
    };
  };

  const getRegistryKey = ({ namespace, taskName }) => {
    return namespace === mainNamespace ? taskName : `${namespace}.${taskName}`;
  };

  const normalizeTaskConfig = (taskName, config) => {
    if (typeof config === 'function') {
      return { handler: config };
    }
    if (config && typeof config === 'object' && !Array.isArray(config)) {
      const { handler, errorHandler, next } = config;
      if (handler != null && typeof handler !== 'function') {
        throw new Error(`任务 ${taskName} 的 handler 必须是一个函数`);
      }
      if (errorHandler != null && typeof errorHandler !== 'function') {
        throw new Error(`任务 ${taskName} 的 errorHandler 必须是一个函数`);
      }
      return { handler, errorHandler, next };
    }
    throw new Error(`任务 ${taskName} 的 handler 必须是一个函数`);
  };

  const registerTaskDeclaration = ({ namespace = mainNamespace, taskName, dirs, scriptName, config, override = false, syncLegacyTask = true, useGlobalDirs = false }) => {
    const normalizedConfig = normalizeTaskConfig(taskName, config);
    const registryKey = getRegistryKey({ namespace, taskName });
    if (!override && options.taskRegistry[registryKey]) {
      return { skipped: true, declaration: options.taskRegistry[registryKey] };
    }
    const declaration = {
      namespace,
      taskName,
      type: registryKey,
      dirs: dirs && dirs.length > 0 ? dirs : options.dirs,
      useGlobalDirs,
      scriptName: scriptName || (namespace === mainNamespace ? options.scriptName : 'index'),
      handler: normalizedConfig.handler,
      errorHandler: normalizedConfig.errorHandler,
      next: normalizedConfig.next
    };
    options.taskRegistry[registryKey] = declaration;
    if (syncLegacyTask && declaration.handler) {
      options.task[registryKey] = declaration.handler;
      if (namespace === mainNamespace) {
        options.task[taskName] = declaration.handler;
      }
    }
    return { skipped: false, declaration };
  };

  const getLegacyDeclaration = type => {
    const config = options.task[type];
    if (typeof config !== 'function' && !(config && typeof config === 'object' && !Array.isArray(config))) {
      return null;
    }
    const { taskName } = splitTaskName(type);
    return registerTaskDeclaration({
      namespace: mainNamespace,
      taskName,
      dirs: options.dirs,
      scriptName: options.scriptName,
      config,
      override: true,
      syncLegacyTask: false,
      useGlobalDirs: true
    }).declaration;
  };

  Object.entries(options.task || {}).forEach(([type, config]) => {
    if (typeof config === 'function' || (config && typeof config === 'object' && !Array.isArray(config))) {
      const { taskName } = splitTaskName(type);
      registerTaskDeclaration({
        namespace: mainNamespace,
        taskName,
        dirs: options.dirs,
        scriptName: options.scriptName,
        config,
        override: true,
        syncLegacyTask: false,
        useGlobalDirs: true
      });
    }
  });

  const resolveDeclarationByNamespace = ({ namespace, taskName }) => {
    const registryKey = getRegistryKey({ namespace, taskName });
    return options.taskRegistry[registryKey] || (namespace === mainNamespace ? getLegacyDeclaration(taskName) : null);
  };

  const resolveTaskDeclaration = (type, { currentDeclaration, forNext = false } = {}) => {
    const { namespace, taskName } = splitTaskName(type);
    if (namespace) {
      if (forNext && currentDeclaration?.namespace !== mainNamespace && namespace === mainNamespace) {
        throw new Error(`任务 ${currentDeclaration.type} 不允许访问主项目任务:${type}`);
      }
      return resolveDeclarationByNamespace({ namespace, taskName });
    }

    if (forNext && currentDeclaration) {
      return resolveDeclarationByNamespace({ namespace: currentDeclaration.namespace, taskName });
    }

    const mainDeclaration = resolveDeclarationByNamespace({ namespace: mainNamespace, taskName });
    if (mainDeclaration) return mainDeclaration;

    const matchedDeclarations = Object.values(options.taskRegistry).filter(declaration => declaration.taskName === taskName);
    if (matchedDeclarations.length === 1) {
      return matchedDeclarations[0];
    }
    return options.taskRegistry[taskName] || getLegacyDeclaration(taskName);
  };

  const getTaskDeclaration = type => {
    const declaration = resolveTaskDeclaration(type);
    if (!declaration) {
      throw new Error('未找到合法的任务声明');
    }
    return declaration;
  };

  const getTaskHandler = declaration => {
    const legacyConfig = options.task[declaration.type] || (declaration.namespace === mainNamespace ? options.task[declaration.taskName] : null);
    if (typeof legacyConfig === 'function') return legacyConfig;
    if (legacyConfig && typeof legacyConfig === 'object' && typeof legacyConfig.handler === 'function') return legacyConfig.handler;
    return declaration.handler;
  };

  const getTaskErrorHandler = declaration => {
    const legacyConfig = options.task[declaration.type] || (declaration.namespace === mainNamespace ? options.task[declaration.taskName] : null);
    if (legacyConfig && typeof legacyConfig === 'object' && typeof legacyConfig.errorHandler === 'function') return legacyConfig.errorHandler;
    return declaration.errorHandler;
  };

  const getTaskNext = declaration => {
    const legacyConfig = options.task[declaration.type] || (declaration.namespace === mainNamespace ? options.task[declaration.taskName] : null);
    if (legacyConfig && typeof legacyConfig === 'object' && Object.prototype.hasOwnProperty.call(legacyConfig, 'next')) return legacyConfig.next;
    return declaration.next;
  };

  const cloneJsonValue = value => {
    if (value == null) return value;
    return JSON.parse(JSON.stringify(value));
  };

  const normalizeError = error => {
    if (error instanceof Error) {
      return (error.stack || error.message || '').replaceAll(process.cwd(), '/server');
    }
    if (typeof error === 'string') {
      return error.replaceAll(process.cwd(), '/server');
    }
    return error;
  };

  const detail = async ({ id }) => {
    const task = await models.task.findByPk(id);
    if (!task) {
      throw new Error('任务不存在');
    }
    return task;
  };

  const handleTaskError = async ({ task, error }) => {
    const declaration = resolveTaskDeclaration(task.type);
    let currentError = error;
    const taskErrorHandler = declaration && getTaskErrorHandler(declaration);
    if (typeof taskErrorHandler === 'function') {
      try {
        await taskErrorHandler({ task, error: currentError, context: task.context });
        return;
      } catch (e) {
        currentError = e;
      }
    }

    if (typeof options.errorHandler === 'function') {
      try {
        await options.errorHandler({ task, error: currentError, context: task.context });
      } catch (e) {
        fastify.log.error(e);
      }
    }
  };

  const failTask = async ({ task, error, updateData = {}, collect = true }) => {
    await task.update(
      Object.assign({}, updateData, {
        status: 'failed',
        error: normalizeError(error),
        completedAt: new Date(),
        startedAt: task.startedAt || task.createdAt
      })
    );
    await handleTaskError({ task, error });
    if (collect) collectTaskStatistics(task);
  };

  const create = async ({ userId, input, type, targetId, targetType, targetName, runnerType, delay = 0, scriptName, priority = 0, parentTaskId, maxRetries = 0, timeout = 60 * 60 * 1000, context, options: currentOptions }) => {
    if (typeof delay !== 'number' || delay < 0) {
      throw new Error('delay 必须为非负数');
    }
    if (typeof priority !== 'number' || !Number.isInteger(priority)) {
      throw new Error('priority 必须为整数');
    }
    if (typeof maxRetries !== 'number' || !Number.isInteger(maxRetries) || maxRetries < 0) {
      throw new Error('maxRetries 必须为非负整数');
    }
    if (typeof timeout !== 'number' || !Number.isInteger(timeout) || timeout < 0) {
      throw new Error('timeout 必须为非负整数');
    }
    const declaration = getTaskDeclaration(type);
    return await models.task.create({
      userId,
      input,
      type: declaration.type,
      targetId,
      targetType,
      targetName,
      runnerType,
      priority,
      parentTaskId,
      maxRetries,
      timeout,
      startTime: delay > 0 ? new Date(Date.now() + 1000 * delay) : new Date(),
      scriptName,
      context,
      options: currentOptions
    });
  };

  const executor = async ({ type, scriptName, ...props }) => {
    const declaration = getTaskDeclaration(type);
    const scriptFile = `${scriptName || declaration.scriptName || 'index'}.js`;
    let taskModulePath = null;
    const dirs = declaration.useGlobalDirs ? options.dirs : declaration.dirs && declaration.dirs.length > 0 ? declaration.dirs : options.dirs;
    for (const dir of dirs) {
      const candidate = path.resolve(dir, declaration.taskName, scriptFile);
      if (await fs.exists(candidate)) {
        taskModulePath = candidate;
        break;
      }
    }
    if (!taskModulePath) {
      throw new Error(`未匹配到任务执行器:${declaration.taskName}/${scriptFile}，已搜索目录:${dirs.join(',')}`);
    }
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

              setTimeout(executePolling, pollInterval);
            } catch (e) {
              reject(e);
            }
          };
          setTimeout(executePolling, pollInterval);
        });
      },
      next: async currentContext => {
        if (typeof props?.task?.update === 'function') {
          await props.task.update({ context: currentContext, status: 'waiting' });
        }
        return false;
      }
    });
  };

  const resolveNextDeclaration = ({ declaration, output }) => {
    const next = getTaskNext(declaration);
    if (!next) return null;
    let nextType;
    if (typeof next === 'string') {
      nextType = next;
    } else if (Array.isArray(next)) {
      nextType = output?.next;
      if (!nextType) {
        throw new Error(`任务 ${declaration.type} 未指定下一个任务`);
      }
      if (!next.includes(nextType)) {
        throw new Error(`任务 ${declaration.type} 指定的下一个任务 ${nextType} 不在允许列表中`);
      }
    } else {
      throw new Error(`任务 ${declaration.type} 的 next 配置必须是字符串或数组`);
    }
    const nextDeclaration = resolveTaskDeclaration(nextType, { currentDeclaration: declaration, forNext: true });
    if (!nextDeclaration) {
      throw new Error(`未找到下一个任务声明:${nextType}`);
    }
    return nextDeclaration;
  };

  const createNextTask = async ({ task, declaration, output }) => {
    const nextDeclaration = resolveNextDeclaration({ declaration, output });
    if (!nextDeclaration) return;
    const input = output && typeof output === 'object' && !Array.isArray(output) ? cloneJsonValue(output) : { data: output };
    if (task.input?.name != null) {
      input.name = task.input.name;
    }
    await create({
      userId: task.userId,
      type: nextDeclaration.type,
      targetId: task.id,
      targetType: 'task',
      targetName: task.input?.name || task.targetName,
      runnerType: task.runnerType,
      input,
      context: cloneJsonValue(task.context || {}),
      parentTaskId: task.id
    });
  };

  const finalizeSuccess = async ({ task, output, handlerResult, userId, updateData = {} }) => {
    const declaration = getTaskDeclaration(task.type);
    const handler = getTaskHandler(declaration);
    if (typeof handler === 'function') {
      await handler({ task, result: handlerResult === undefined ? output : handlerResult, context: task.context });
    }
    await createNextTask({ task, declaration, output });
    await task.update(
      Object.assign({}, updateData, {
        status: 'success',
        output,
        progress: 100,
        completedAt: new Date(),
        startedAt: task.startedAt || task.createdAt,
        completedUserId: userId
      })
    );
    collectTaskStatistics(task);
    await fastify[options.name].services.triggerChildTasks(task);
  };

  return {
    fastify,
    options,
    models,
    Op,
    mainNamespace,
    collectTaskStatistics,
    verifySignature,
    normalizeDirs,
    registerTaskDeclaration,
    getTaskDeclaration,
    resolveTaskDeclaration,
    detail,
    failTask,
    create,
    executor,
    finalizeSuccess
  };
};

const getTaskServiceContext = (fastify, options) => {
  const namespace = fastify[options.name];
  if (!namespace[CONTEXT_KEY]) {
    namespace[CONTEXT_KEY] = createContext(fastify, options);
  }
  return namespace[CONTEXT_KEY];
};

module.exports = getTaskServiceContext;

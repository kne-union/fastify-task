const fp = require('fastify-plugin');
const getTaskServiceContext = require('../helpers/task-service-context');

module.exports = fp(async (fastify, options) => {
  const context = getTaskServiceContext(fastify, options);

  const processNext = async ({ id, signature, result: resultStr }) => {
    const task = await context.detail({ id });
    if (task.status !== 'waiting') {
      throw new Error('当前任务状态不允许执行Next操作');
    }
    if (task.context?.secret && !context.verifySignature({ secret: task.context.secret, id, data: resultStr, signature })) {
      throw new Error('签名验证失败');
    }
    let result;
    try {
      result = JSON.parse(resultStr);
    } catch (e) {
      await context.failTask({ task, error: e });
      throw e;
    }
    if (result.code !== 0) {
      await context.failTask({ task, error: result });
      return;
    }
    try {
      await context.finalizeSuccess({ task, output: result, handlerResult: result.data });
    } catch (e) {
      await context.failTask({ task, error: e, updateData: { output: result } });
      throw e;
    }
  };

  const log = async ({ id, taskId, data, message = '' }) => {
    const targetId = id || taskId;
    const task = await context.detail({ id: targetId });

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
    const task = await context.detail({ id: targetId });

    if (task.context?.secret && !context.verifySignature({ secret: task.context.secret, id: targetId, data: { data, message }, signature })) {
      throw new Error('签名验证失败');
    }

    return log({ id: targetId, data, message });
  };

  const ACTIVE_STATUSES = ['pending', 'running', 'waiting'];

  const normalizeProgress = progress => {
    if (progress == null) return null;
    if (typeof progress !== 'number' || !Number.isFinite(progress) || progress < 0 || progress > 100) {
      throw new Error('progress 必须是 0-100 之间的数字');
    }
    return progress;
  };

  // 进度上报：只更新进度并记日志，任务保持原状态，不触发 handler / next / errorHandler
  const reportProgress = async ({ id, code, data, message, progress }) => {
    const task = await context.detail({ id });
    const updated = await context.updateTaskByStatus({
      task,
      allowedStatuses: ACTIVE_STATUSES,
      updateData: { progress: Math.round(progress) }
    });
    if (!updated) {
      return;
    }
    await log({ id, message: message || '进度回调', data: { code, data, message, progress } });
  };

  const callback = async ({ id, code, data, message, progress }) => {
    await context.detail({ id });

    const currentProgress = normalizeProgress(progress);
    // code 为 0 且 progress 小于 100 视为进度上报；不带 progress 或 progress 为 100 时与原来一致，按最终结果处理
    if (code === 0 && currentProgress != null && currentProgress < 100) {
      return reportProgress({ id, code, data, message, progress: currentProgress });
    }

    const result = { code, data, message };
    await log({ id, message: '回调结果', data: result });
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
    await fastify[options.name].services.complete(input);
  };

  const callbackWithSignature = async ({ id, taskId, code, data, message, progress, signature }) => {
    const targetId = id || taskId;
    const task = await context.detail({ id: targetId });

    // progress 为 undefined 时 JSON 序列化会忽略该字段，签名内容与不带进度的旧回调完全一致
    if (task.context?.secret && !context.verifySignature({ secret: task.context.secret, id: targetId, data: { code, data, message, progress }, signature })) {
      throw new Error('签名验证失败');
    }

    return callback({ id: targetId, code, data, message, progress });
  };

  Object.assign(fastify[options.name].services, {
    log,
    logWithSignature,
    callback,
    callbackWithSignature,
    processNext
  });
});

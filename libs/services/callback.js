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
    const result = JSON.parse(resultStr);
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

  const callback = async ({ id, code, data, message }) => {
    await context.detail({ id });

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
    await fastify[options.name].services.complete(input);
  };

  const callbackWithSignature = async ({ id, code, data, message, signature }) => {
    const task = await context.detail({ id });

    if (task.context?.secret && !context.verifySignature({ secret: task.context.secret, id, data: { code, data, message }, signature })) {
      throw new Error('签名验证失败');
    }

    return callback({ id, code, data, message });
  };

  Object.assign(fastify[options.name].services, {
    log,
    logWithSignature,
    callback,
    callbackWithSignature,
    processNext
  });
});

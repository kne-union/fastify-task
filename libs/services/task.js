const fp = require('fastify-plugin');
const getTaskServiceContext = require('../helpers/task-service-context');

module.exports = fp(async (fastify, options) => {
  const context = getTaskServiceContext(fastify, options);
  const { models, Op } = context;
  const SORTABLE_FIELDS = new Set(['id', 'targetId', 'targetType', 'type', 'status', 'runnerType', 'priority', 'createdAt', 'updatedAt', 'startTime', 'startedAt', 'completedAt']);

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
        context.collectTaskStatistics(Object.assign({}, row, { status: 'canceled', completedAt }));
      }
      return affectedCount;
    }
    if (id) {
      const task = await context.detail({ id });
      const transitioned = await context.updateTaskByStatus({
        task,
        allowedStatuses: ['pending', 'running'],
        updateData: {
          status: 'canceled',
          completedAt: new Date()
        }
      });
      if (!transitioned) {
        return;
      }
      context.collectTaskStatistics(task);
    }
  };

  const complete = async ({ id, userId, output, ...props }) => {
    const task = await context.detail({ id });
    if (props.status === 'success') {
      try {
        await context.finalizeSuccess({ task, output, userId });
      } catch (e) {
        await context.failTask({ task, error: e, updateData: Object.assign({}, props, { output, completedUserId: userId }) });
        throw e;
      }
    } else {
      await context.failTask({ task, error: props.error || '任务失败', updateData: Object.assign({}, props, { completedUserId: userId }) });
    }
  };

  const waitingComplete = async ({ id, pollInterval = 1000, maxPollTimes = 20 }) => {
    let task = await context.detail({ id });

    if (task.status === 'pending') {
      await context.updateTaskByStatus({
        task,
        allowedStatuses: ['pending'],
        updateData: { priority: Number.MAX_SAFE_INTEGER }
      });
      const claimedTask = await fastify[options.name].services.claimTask(task);
      if (claimedTask) {
        await fastify[options.name].services.processSystemTask(claimedTask, { claimed: true });
        task = claimedTask;
      }
      await task.reload();
    }

    if (task.status === 'success') {
      return task.output;
    }
    if (task.status === 'failed') {
      throw new Error(task.error || '任务失败');
    }
    if (task.status === 'canceled') {
      throw new Error(task.error || '任务取消');
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

          setTimeout(executePolling, pollInterval);
        } catch (e) {
          reject(e);
        }
      };
      setTimeout(executePolling, pollInterval);
    });
  };

  const getTimeQuery = fieldValue => {
    const { startTime, endTime } = fieldValue;
    if (!!startTime && !!endTime) {
      return {
        [Op.between]: [new Date(startTime), new Date(endTime)]
      };
    } else if (!!startTime) {
      return {
        [Op.gte]: new Date(startTime)
      };
    } else if (!!endTime) {
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

    if (filter && filter.createdAt) {
      whereQuery.createdAt = getTimeQuery(filter.createdAt);
    }

    if (filter && filter.completedAt) {
      whereQuery.completedAt = getTimeQuery(filter.completedAt);
    }

    let order = [['createdAt', 'DESC']];

    if (sort && Object.keys(sort).length > 0) {
      order = Object.entries(sort).map(([key, direction]) => {
        if (!SORTABLE_FIELDS.has(key)) {
          throw new Error(`不支持的排序字段:${key}`);
        }
        const normalizedDirection = String(direction || '').toUpperCase();
        if (!['ASC', 'DESC'].includes(normalizedDirection)) {
          throw new Error(`不支持的排序方向:${direction}`);
        }
        return [key, normalizedDirection];
      });
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
    const task = await context.detail({ id });
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

  Object.assign(fastify[options.name].services, {
    create: context.create,
    detail: context.detail,
    list,
    complete,
    cancel,
    retry,
    waitingComplete
  });
});

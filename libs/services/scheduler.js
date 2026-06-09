const fp = require('fastify-plugin');
const getTaskServiceContext = require('../helpers/task-service-context');

module.exports = fp(async (fastify, options) => {
  const context = getTaskServiceContext(fastify, options);
  const { models, Op } = context;

  const getRunningRecoveryBefore = () => {
    const recoverAfter = Number.isInteger(options.recoverRunningTaskAfter) && options.recoverRunningTaskAfter >= 0 ? options.recoverRunningTaskAfter : 30 * 60 * 1000;
    return new Date(Date.now() - recoverAfter);
  };

  const getRunningResetWhere = ({ staleOnly = false, before } = {}) => {
    const where = { status: 'running' };
    if (staleOnly) {
      where[Op.or] = [{ startedAt: { [Op.lte]: before || getRunningRecoveryBefore() } }, { startedAt: null }];
    }
    return where;
  };

  const resetAll = async (props = {}) => {
    const [affectedCount] = await models.task.update({ status: 'pending' }, { where: getRunningResetWhere(props) });
    return affectedCount;
  };

  const claimTask = async (task, { allowChild = false } = {}) => {
    const now = new Date();
    const [affectedCount] = await models.task.update(
      {
        status: 'running',
        startedAt: now,
        progress: 0,
        error: null,
        output: null,
        pollCount: 0,
        pollResults: [],
        completedAt: null
      },
      {
        where: Object.assign(
          {
            id: task.id,
            runnerType: 'system',
            status: 'pending',
            startTime: {
              [Op.lte]: now
            }
          },
          allowChild ? {} : { parentTaskId: null }
        )
      }
    );
    if (affectedCount !== 1) return null;
    return await models.task.findByPk(task.id);
  };

  const processSystemTask = async (task, { claimed = false } = {}) => {
    if (!claimed) {
      task = await claimTask(task, { allowChild: true });
      if (!task) return false;
    }
    const addLog = props => {
      return fastify[options.name].services.log(Object.assign({}, props, { taskId: task.id }));
    };

    const createTimeout = ms => new Promise((_, reject) => setTimeout(() => reject(new Error(`任务执行超时(${ms}ms)`)), ms));

    const executorPromise = context.executor({ type: task.type, scriptName: task.scriptName, task, log: addLog });
    const racePromise = options.taskTimeout > 0 ? Promise.race([executorPromise, createTimeout(options.taskTimeout)]) : executorPromise;

    return racePromise
      .then(async result => {
        if (result === false) {
          return;
        }
        await context.finalizeSuccess({ task, output: result });
      })
      .catch(async e => {
        const currentRetryCount = (task.retryCount || 0) + 1;
        const maxRetries = task.maxRetries || 0;
        if (currentRetryCount <= maxRetries) {
          const baseDelay = options.retryBaseDelay || 5000;
          const backoffDelay = baseDelay * Math.pow(2, currentRetryCount - 1);
          const retryScheduled = await context.updateTaskByStatus({
            task,
            allowedStatuses: ['running'],
            updateData: {
              status: 'pending',
              retryCount: currentRetryCount,
              error: (e.stack || '').replaceAll(process.cwd(), '/server'),
              startTime: new Date(Date.now() + backoffDelay),
              completedAt: null
            }
          });
          if (retryScheduled) {
            fastify.log.info(`任务 ${task.id} 第 ${currentRetryCount} 次重试，${backoffDelay}ms 后执行`);
          }
        } else {
          await context.failTask({ task, error: e, updateData: { retryCount: currentRetryCount } });
        }
      });
  };

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
    }
  };

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
      return elapsed > task.timeout;
    });

    if (tasksToFail.length === 0) return;

    fastify.log.info(`检测到 ${tasksToFail.length} 个超时任务`);

    for (const task of tasksToFail) {
      const elapsed = now.getTime() - new Date(task.startedAt).getTime();
      await context.failTask({ task, error: `任务超时：已执行 ${elapsed}ms，超过设定的 ${task.timeout}ms 超时时间`, updateData: { completedAt: now } });
    }
  };

  const claimPendingTasks = async limit => {
    if (limit <= 0) return [];
    const now = new Date();
    const pendingTasks = await models.task.findAll({
      where: {
        runnerType: 'system',
        status: 'pending',
        parentTaskId: null,
        startTime: {
          [Op.lte]: now
        }
      },
      order: [
        ['priority', 'DESC'],
        ['startTime', 'ASC']
      ],
      limit: limit
    });

    const claimedTasks = [];
    for (const task of pendingTasks) {
      const [affectedCount] = await models.task.update(
        {
          status: 'running',
          startedAt: now,
          progress: 0,
          error: null,
          output: null,
          pollCount: 0,
          pollResults: [],
          completedAt: null
        },
        {
          where: {
            id: task.id,
            runnerType: 'system',
            status: 'pending',
            parentTaskId: null,
            startTime: {
              [Op.lte]: now
            }
          }
        }
      );
      if (affectedCount === 1) {
        const claimedTask = await models.task.findByPk(task.id);
        if (claimedTask) {
          claimedTasks.push(claimedTask);
        }
      }
    }
    return claimedTasks;
  };

  const runner = async () => {
    await checkTimeout();

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
    const pendingTasks = await claimPendingTasks(limit);

    if (pendingTasks.length > 0) {
      fastify.log.info(`本轮执行 ${pendingTasks.length} 个待处理的系统任务`);
      await Promise.all(pendingTasks.map(async task => processSystemTask(task, { claimed: true })));
    }
  };

  Object.assign(fastify[options.name].services, {
    runner,
    resetAll,
    claimPendingTasks,
    claimTask,
    triggerChildTasks,
    processSystemTask
  });
});

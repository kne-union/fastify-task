const { TaskExecutionError } = require('../errors');
const { CONFIG_CONSTANTS } = require('../config');

/**
 * 任务处理器模块
 * 负责任务的处理逻辑，包括重试、超时等
 */
class TaskProcessor {
  constructor(fastify, config, models, executor, collectTaskStatistics, triggerChildTasks) {
    this.fastify = fastify;
    this.config = config;
    this.models = models;
    this.executor = executor;
    this.collectTaskStatistics = collectTaskStatistics;
    this.triggerChildTasks = triggerChildTasks;
  }

  /**
   * 处理系统任务
   * @param {Object} task - 任务对象
   * @returns {Promise<void>}
   */
  async processSystemTask(task) {
    await task.update({
      status: CONFIG_CONSTANTS.TASK_STATUSES.RUNNING,
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
      return this.fastify[`${this.config.name}`].services.log(Object.assign({}, props, { taskId: task.id }));
    };

    // 任务超时处理
    const createTimeout = ms => new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`任务执行超时(${ms}ms)`)), ms)
    );

    const executorPromise = this.executor.execute({
      type: task.type,
      scriptName: task.scriptName,
      task,
      log: addLog
    });

    const racePromise = this.config.taskTimeout > 0
      ? Promise.race([executorPromise, createTimeout(this.config.taskTimeout)])
      : executorPromise;

    return racePromise
      .then(async result => {
        if (result === false) {
          return;
        }

        await this.config.task[task.type]({ task, result, context: task.context });

        await task.update({
          status: CONFIG_CONSTANTS.TASK_STATUSES.SUCCESS,
          output: result,
          progress: 100,
          completedAt: new Date()
        });

        this.collectTaskStatistics(task);

        // 父任务成功后激活子任务
        await this.triggerChildTasks(task);
      })
      .catch(async error => {
        // 判断是否自动重试
        const currentRetryCount = (task.retryCount || 0) + 1;
        const maxRetries = task.maxRetries || 0;

        if (currentRetryCount < maxRetries) {
          // 指数退避：baseDelay * 2^retryCount
          const baseDelay = this.config.retryBaseDelay || CONFIG_CONSTANTS.RETRY_BASE_DELAY;
          const backoffDelay = baseDelay * Math.pow(2, currentRetryCount - 1);

          await task.update({
            status: CONFIG_CONSTANTS.TASK_STATUSES.PENDING,
            retryCount: currentRetryCount,
            error: this.sanitizeStackTrace(error),
            startTime: new Date(Date.now() + backoffDelay),
            completedAt: null
          });

          this.fastify.log.info(`任务 ${task.id} 第 ${currentRetryCount} 次重试，${backoffDelay}ms 后执行`);
        } else {
          await task.update({
            status: CONFIG_CONSTANTS.TASK_STATUSES.FAILED,
            error: this.sanitizeStackTrace(error),
            completedAt: new Date(),
            retryCount: currentRetryCount
          });

          this.collectTaskStatistics(task);
        }
      });
  }

  /**
   * 净化错误堆栈信息
   * @param {Error} error - 错误对象
   * @returns {string} 净化后的堆栈信息
   */
  sanitizeStackTrace(error) {
    if (!error || !error.stack) {
      return '';
    }
    return error.stack.replaceAll(process.cwd(), '/server');
  }

  /**
   * 触发子任务
   * @param {Object} parentTask - 父任务
   * @returns {Promise<void>}
   */
  async triggerChildTasks(parentTask) {
    const childTasks = await this.models.task.findAll({
      where: {
        parentTaskId: parentTask.id,
        status: CONFIG_CONSTANTS.TASK_STATUSES.PENDING
      }
    });

    if (childTasks.length === 0) return;

    this.fastify.log.info(`父任务 ${parentTask.id} 完成，激活 ${childTasks.length} 个子任务`);

    for (const child of childTasks) {
      if (child.runnerType === CONFIG_CONSTANTS.RUNNER_TYPES.SYSTEM) {
        await this.processSystemTask(child);
      }
      // manual 类型子任务保持 pending，等待手动执行
    }
  }

  /**
   * 检查超时任务
   * @returns {Promise<void>}
   */
  async checkTimeout() {
    const now = new Date();
    const { Op } = this.fastify.sequelize.Sequelize;

    const timedOutTasks = await this.models.task.findAll({
      where: {
        status: { [Op.in]: [CONFIG_CONSTANTS.TASK_STATUSES.RUNNING, CONFIG_CONSTANTS.TASK_STATUSES.WAITING] },
        timeout: { [Op.gt]: 0 },
        startedAt: { [Op.ne]: null }
      }
    });

    const tasksToFail = timedOutTasks.filter(task => {
      const elapsed = now.getTime() - new Date(task.startedAt).getTime();
      return elapsed > task.timeout * 60 * 1000;
    });

    if (tasksToFail.length === 0) return;

    this.fastify.log.info(`检测到 ${tasksToFail.length} 个超时任务`);

    for (const task of tasksToFail) {
      const elapsed = now.getTime() - new Date(task.startedAt).getTime();
      const prevStatus = task.status;

      await task.update({
        status: CONFIG_CONSTANTS.TASK_STATUSES.FAILED,
        error: `任务超时：已执行 ${Math.round(elapsed / 60000)} 分钟，超过设定的 ${task.timeout} 分钟超时时间`,
        completedAt: now
      });

      this.collectTaskStatistics(task);
    }
  }
}

module.exports = TaskProcessor;
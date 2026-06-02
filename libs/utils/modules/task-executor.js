const { TaskExecutionError, TaskTimeoutError } = require('../errors');
const { CONFIG_CONSTANTS } = require('../config');

/**
 * 任务执行器模块
 * 负责任务的实际执行逻辑
 */
class TaskExecutor {
  constructor(fastify, config, models, securityValidator, cacheManager) {
    this.fastify = fastify;
    this.config = config;
    this.models = models;
    this.securityValidator = securityValidator;
    this.cacheManager = cacheManager;
    this.path = require('node:path');
    this.fs = require('fs-extra');
  }

  /**
   * 执行任务
   * @param {Object} params - 执行参数
   * @returns {*} 执行结果
   */
  async execute({ type, scriptName, ...props }) {
    try {
      // 验证任务类型
      this.securityValidator.validateTaskType(type, Object.keys(this.config.task || {}));

      const scriptFile = `${scriptName || this.config.scriptName}${CONFIG_CONSTANTS.SCRIPT_EXTENSION}`;
      let taskModulePath = null;

      // 安全的路径查找和验证
      for (const dir of this.config.dirs) {
        try {
          const candidate = this.path.join(dir, type, scriptFile);
          const validatedPath = this.securityValidator.validateScriptPath(dir, this.path.join(type, scriptFile));

          if (await this.fs.exists(validatedPath)) {
            taskModulePath = validatedPath;
            break;
          }
        } catch (error) {
          // 跳过无效路径，继续尝试其他目录
          this.fastify.log.debug(`路径验证失败: ${error.message}`);
          continue;
        }
      }

      if (!taskModulePath) {
        throw new Error(`未匹配到任务执行器:${type}/${scriptFile}，已搜索目录:${this.config.dirs.join(',')}`);
      }

      // 清除 require 缓存，确保运行期间模块更新后能加载最新版本
      delete require.cache[taskModulePath];

      return await require(taskModulePath)(this.fastify, this.config, this.createExecutionContext(props));
    } catch (error) {
      throw new TaskExecutionError(props.task?.id, error, { operation: 'executor' });
    }
  }

  /**
   * 创建执行上下文
   * @param {Object} props - 属性
   * @returns {Object} 执行上下文
   */
  createExecutionContext(props) {
    return {
      ...props,
      updateProgress: this.createProgressUpdater(props),
      polling: this.createPollingFunction(props),
      next: this.createNextFunction(props)
    };
  }

  /**
   * 创建进度更新器
   * @param {Object} props - 属性
   * @returns {Function} 进度更新函数
   */
  createProgressUpdater(props) {
    return async (progress) => {
      try {
        const validatedProgress = this.securityValidator.validateNumber(progress, {
          min: 0,
          max: 100,
          integer: true
        });

        if (typeof props?.task?.update === 'function') {
          await props.task.update({ progress: validatedProgress });

          // 更新缓存
          this.cacheManager.set(`task:${props.task.id}`, props.task, 60000);
        }
      } catch (error) {
        this.fastify.log.warn(`进度更新失败: ${error.message}`);
      }
    };
  }

  /**
   * 创建轮询函数
   * @param {Object} props - 属性
   * @returns {Function} 轮询函数
   */
  createPollingFunction(props) {
    return async (callback, currentOptions) => {
      const maxPollTimes = this.securityValidator.validateNumber(
        currentOptions?.maxPollTimes || this.config.maxPollTimes,
        { min: 1, max: 1000, integer: true }
      );

      const pollInterval = this.securityValidator.validateNumber(
        currentOptions?.pollInterval || this.config.pollInterval,
        { min: CONFIG_CONSTANTS.POLL_MIN_INTERVAL_MS, max: CONFIG_CONSTANTS.POLL_MAX_INTERVAL_MS }
      );

      let pollCount = 0;
      return await new Promise((resolve, reject) => {
        const executePolling = async () => {
          try {
            pollCount++;
            if (pollCount > maxPollTimes) {
              reject(new TaskTimeoutError(props.task?.id, maxPollTimes * pollInterval, {
                reason: `轮询超时（${maxPollTimes}次），任务未完成`
              }));
              return;
            }

            const pollingResult = Object.assign({}, await callback());
            const { result, data, message, progress } = pollingResult;

            if (typeof props?.task?.update === 'function') {
              await props.task.reload();
              const limitedPollResults = this.securityValidator.limitLogsSize(
                [...props.task.pollResults, Object.assign({}, pollingResult, { time: new Date() })],
                CONFIG_CONSTANTS.LOGS_MAX_SIZE
              );

              await props.task.update(
                Object.assign(
                  {},
                  {
                    pollCount: props.task.pollCount + 1,
                    pollResults: limitedPollResults
                  },
                  Number.isInteger(progress) ? { progress } : {}
                )
              );

              // 更新缓存
              this.cacheManager.set(`task:${props.task.id}`, props.task, 60000);
            }

            if (result === 'failed') {
              reject(new TaskExecutionError(props.task?.id, new Error(message), { reason: '任务处理失败' }));
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
    };
  }

  /**
   * 创建 next 函数
   * @param {Object} props - 属性
   * @returns {Function} next 函数
   */
  createNextFunction(props) {
    return async (context) => {
      if (typeof props?.task?.update === 'function') {
        await props.task.update({ context, status: CONFIG_CONSTANTS.TASK_STATUSES.WAITING });

        // 更新缓存
        this.cacheManager.set(`task:${props.task.id}`, props.task, 60000);
      }
      return false;
    };
  }
}

module.exports = TaskExecutor;
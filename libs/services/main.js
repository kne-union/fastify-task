const fp = require('fastify-plugin');
const path = require('node:path');
const fs = require('fs-extra');
const crypto = require('node:crypto');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');

dayjs.extend(utc);
dayjs.extend(timezone);

module.exports = fp(async (fastify, options) => {
  const { models } = fastify[options.name];
  const { Op } = fastify.sequelize.Sequelize;

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
      updateDailyStatistics(task);
      return;
    }
    await options.task[task.type]({ task, result: result.data, context: task.context });
    await task.update({
      status: 'success',
      output: result,
      progress: 100,
      completedAt: new Date()
    });
    updateDailyStatistics(task);
  };

  const processSystemTask = async task => {
    try {
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
      executor({ type: task.type, scriptName: task.scriptName, task, log: addLog })
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
          updateDailyStatistics(task);
        })
        .catch(async e => {
          await task.update({
            status: 'failed',
            error: (e.stack || '').replaceAll(process.cwd(), '/server'),
            completedAt: new Date()
          });
          updateDailyStatistics(task);
        });
    } catch (e) {
      await task.update({
        status: 'failed',
        error: (e.stack || '').replaceAll(process.cwd(), '/server'),
        completedAt: new Date()
      });
      updateDailyStatistics(task);
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
      const [affectedCount] = await models.task.update(
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
      updateDailyStatistics(task);
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
          startedAt: task.startedAt || task.createdAt,
          completedUserId: userId
        });
        updateDailyStatistics(task);
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
        updateDailyStatistics(task);
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
      updateDailyStatistics(task);
    }
  };

  const waitingComplete = async ({ id, pollInterval = 1000, maxPollTimes = 20 }) => {
    const task = await detail({ id });
    if (!task) {
      throw new Error('任务不存在');
    }

    if (task.status === 'pending') {
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

  const log = async ({ id, taskId, data, message = '' }) => {
    const targetId = id || taskId;
    const task = await detail({ id: targetId });
    if (!task) {
      throw new Error('任务不存在');
    }

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
    if (!task) {
      throw new Error('任务不存在');
    }

    if (task.context?.secret && !verifySignature({ secret: task.context.secret, id: targetId, data: { data, message }, signature })) {
      throw new Error('签名验证失败');
    }

    return log({ id: targetId, data, message });
  };

  const callback = async ({ id, code, data, message }) => {
    const task = await detail({ id });
    if (!task) {
      throw new Error('任务不存在');
    }

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
    if (!task) {
      throw new Error('任务不存在');
    }

    if (task.context?.secret && !verifySignature({ secret: task.context.secret, id, data: { code, data, message }, signature })) {
      throw new Error('签名验证失败');
    }

    return callback({ id, code, data, message });
  };

  // 按指定时区获取"今天0点"的UTC Date对象
  const getTodayStart = tz => {
    if (!tz) {
      return dayjs().startOf('day').toDate();
    }
    return dayjs().tz(tz).startOf('day').toDate();
  };

  // 按指定时区格式化日期
  const formatDate = (date, tz) => {
    if (!tz) {
      return dayjs(date).format('YYYY-MM-DD');
    }
    return dayjs(date).tz(tz).format('YYYY-MM-DD');
  };

  const resolveTimezone = tz => tz || dayjs.tz.guess();

  /** 与 updateDailyStatistics 一致的耗时计算（用于实时接口在无日统计时的回算） */
  const computeTaskTiming = task => {
    const completedAt = task.completedAt ? new Date(task.completedAt) : null;
    const createdAt = task.createdAt ? new Date(task.createdAt) : null;
    const startedAt = task.startedAt ? new Date(task.startedAt) : null;
    let waitingTime = 0;
    let executionTime = 0;
    let totalTime = 0;
    let hasTiming = false;
    if (createdAt && completedAt && !Number.isNaN(createdAt.getTime()) && !Number.isNaN(completedAt.getTime())) {
      totalTime = completedAt.getTime() - createdAt.getTime();
      if (startedAt && !Number.isNaN(startedAt.getTime())) {
        waitingTime = startedAt.getTime() - createdAt.getTime();
        executionTime = completedAt.getTime() - startedAt.getTime();
      } else {
        executionTime = totalTime;
      }
      hasTiming = totalTime > 0;
    }
    return { waitingTime, executionTime, totalTime, hasTiming };
  };

  // 更新每日统计表（任务完成/取消时调用，异步不阻塞主流程）
  const updateDailyStatistics = task => {
    const completedAt = task.completedAt || new Date();
    const statDate = dayjs(completedAt).format('YYYY-MM-DD');
    const status = task.status;

    // 计算耗时
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
        // 无startedAt时(手动任务直接完成)，等待时间=0，执行时间=总耗时
        executionTime = totalTime;
      }
      hasTiming = totalTime > 0;
    }

    // 异步更新，不阻塞主流程
    (async () => {
      try {
        const [stat, created] = await models.taskDailyStatistics.findOrCreate({
          where: { date: statDate },
          defaults: {
            date: statDate,
            totalCompleted: 1,
            successCount: status === 'success' ? 1 : 0,
            failedCount: status === 'failed' ? 1 : 0,
            canceledCount: status === 'canceled' ? 1 : 0,
            totalWaitingTime: hasTiming ? waitingTime : 0,
            totalExecutionTime: hasTiming ? executionTime : 0,
            totalTime: hasTiming ? totalTime : 0,
            timedTaskCount: hasTiming ? 1 : 0,
            byType: {
              [task.type]: {
                count: 1,
                successCount: status === 'success' ? 1 : 0,
                failedCount: status === 'failed' ? 1 : 0,
                canceledCount: status === 'canceled' ? 1 : 0,
                totalWaitingTime: hasTiming ? waitingTime : 0,
                totalExecutionTime: hasTiming ? executionTime : 0,
                totalTime: hasTiming ? totalTime : 0,
                timedTaskCount: hasTiming ? 1 : 0
              }
            },
            byRunnerType: {
              [task.runnerType]: {
                count: 1,
                successCount: status === 'success' ? 1 : 0,
                failedCount: status === 'failed' ? 1 : 0,
                canceledCount: status === 'canceled' ? 1 : 0,
                totalWaitingTime: hasTiming ? waitingTime : 0,
                totalExecutionTime: hasTiming ? executionTime : 0,
                totalTime: hasTiming ? totalTime : 0,
                timedTaskCount: hasTiming ? 1 : 0
              }
            }
          }
        });

        if (!created) {
          const mergeKey = (obj, key, increment) => {
            const current = obj[key] || {
              count: 0, successCount: 0, failedCount: 0, canceledCount: 0,
              totalWaitingTime: 0, totalExecutionTime: 0, totalTime: 0, timedTaskCount: 0
            };
            return Object.assign({}, obj, {
              [key]: {
                count: current.count + (increment.count || 0),
                successCount: current.successCount + (increment.successCount || 0),
                failedCount: current.failedCount + (increment.failedCount || 0),
                canceledCount: current.canceledCount + (increment.canceledCount || 0),
                totalWaitingTime: current.totalWaitingTime + (increment.totalWaitingTime || 0),
                totalExecutionTime: current.totalExecutionTime + (increment.totalExecutionTime || 0),
                totalTime: current.totalTime + (increment.totalTime || 0),
                timedTaskCount: current.timedTaskCount + (increment.timedTaskCount || 0)
              }
            });
          };

          const typeIncrement = {
            count: 1,
            successCount: status === 'success' ? 1 : 0,
            failedCount: status === 'failed' ? 1 : 0,
            canceledCount: status === 'canceled' ? 1 : 0,
            totalWaitingTime: hasTiming ? waitingTime : 0,
            totalExecutionTime: hasTiming ? executionTime : 0,
            totalTime: hasTiming ? totalTime : 0,
            timedTaskCount: hasTiming ? 1 : 0
          };

          await stat.update({
            totalCompleted: stat.totalCompleted + 1,
            successCount: stat.successCount + (status === 'success' ? 1 : 0),
            failedCount: stat.failedCount + (status === 'failed' ? 1 : 0),
            canceledCount: stat.canceledCount + (status === 'canceled' ? 1 : 0),
            totalWaitingTime: stat.totalWaitingTime + (hasTiming ? waitingTime : 0),
            totalExecutionTime: stat.totalExecutionTime + (hasTiming ? executionTime : 0),
            totalTime: stat.totalTime + (hasTiming ? totalTime : 0),
            timedTaskCount: stat.timedTaskCount + (hasTiming ? 1 : 0),
            byType: mergeKey(stat.byType || {}, task.type, typeIncrement),
            byRunnerType: mergeKey(stat.byRunnerType || {}, task.runnerType, typeIncrement)
          });
        }
      } catch (e) {
        fastify.log.error(`更新每日统计失败: ${e.message}`);
      }
    })();
  };

  const statistics = {
    getOverview: async ({ range = '7d', timezone, type, runnerType } = {}) => {
      const { Sequelize } = models.task.sequelize;
      const taskModel = models.task;
      const createdAtCol = taskModel.rawAttributes.createdAt.field;
      const dialect = taskModel.sequelize.getDialect();
      const effectiveTimezone = resolveTimezone(timezone);

      const rangeMap = {
        '7d': { days: 7, label: '近7天' },
        '1m': { days: 30, label: '近1个月' },
        '1y': { days: 365, label: '近1年' }
      };
      const normalizedRange = rangeMap[range] ? range : '7d';
      const rangeConfig = rangeMap[normalizedRange];
      const todayStart = getTodayStart(effectiveTimezone);
      const startDate = new Date(todayStart);
      startDate.setDate(startDate.getDate() - rangeConfig.days);

      const whereRange = { createdAt: { [Sequelize.Op.gte]: startDate } };
      if (type) {
        whereRange.type = type;
      }
      if (runnerType) {
        whereRange.runnerType = runnerType;
      }

      // 按时区格式化日期的SQL
      const dateFn = (() => {
        if (!effectiveTimezone || dialect === 'sqlite') {
          return col => Sequelize.fn('DATE', Sequelize.col(col));
        }
        if (dialect === 'postgres') {
          return col => Sequelize.fn('TO_CHAR', Sequelize.literal(`"${col}" AT TIME ZONE '${effectiveTimezone}'`), 'YYYY-MM-DD');
        }
        return col => Sequelize.fn('DATE_FORMAT', Sequelize.fn('CONVERT_TZ', Sequelize.col(col), '+00:00', Sequelize.literal(`'${effectiveTimezone}'`)), '%Y-%m-%d');
      })();

      // 并行执行所有查询
      const [totalTasks, byStatus, byType, byRunnerType, byTargetType, recentTrend, recentTrendByStatus, recentTrendByType] = await Promise.all([
        taskModel.count({ where: whereRange }),
        taskModel.findAll({
          attributes: ['status', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereRange,
          group: ['status'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['type', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereRange,
          group: ['type'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['runnerType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereRange,
          group: ['runnerType'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['targetType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereRange,
          group: ['targetType'],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [dateFn(createdAtCol), 'date'],
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereRange,
          group: [dateFn(createdAtCol)],
          order: [[dateFn(createdAtCol), 'ASC']],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [dateFn(createdAtCol), 'date'],
            'status',
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereRange,
          group: [dateFn(createdAtCol), 'status'],
          order: [[dateFn(createdAtCol), 'ASC']],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [dateFn(createdAtCol), 'date'],
            'type',
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereRange,
          group: [dateFn(createdAtCol), 'type'],
          order: [[dateFn(createdAtCol), 'ASC']],
          raw: true
        })
      ]);

      // SQLite 需要在JS层做时区偏移修正DATE结果
      const adjustDate = dateStr => {
        if (!effectiveTimezone || dialect !== 'sqlite') return dateStr;
        return dayjs.utc(dateStr).tz(effectiveTimezone).format('YYYY-MM-DD');
      };

      // 从预聚合表读取每日耗时统计
      const dailyStats = await models.taskDailyStatistics.findAll({
        where: { date: { [Sequelize.Op.gte]: dayjs(startDate).format('YYYY-MM-DD') } },
        order: [['date', 'ASC']],
        raw: true
      });

      const durationTrend = dailyStats.map(item => {
        // 如果指定了type/runnerType筛选，从对应子维度中提取数据作为主数据
        const typeData = type && item.byType && item.byType[type] ? item.byType[type] : null;
        const runnerTypeData = runnerType && item.byRunnerType && item.byRunnerType[runnerType] ? item.byRunnerType[runnerType] : null;
        // runnerType优先级高于type（两者同时指定时取交集，但预聚合表无交叉维度，取runnerType）
        const filterData = runnerTypeData || typeData;
        const mainTimedTaskCount = filterData ? filterData.timedTaskCount : item.timedTaskCount;
        const mainTotalWaitingTime = filterData ? filterData.totalWaitingTime : item.totalWaitingTime;
        const mainTotalExecutionTime = filterData ? filterData.totalExecutionTime : item.totalExecutionTime;
        const mainTotalTime = filterData ? filterData.totalTime : item.totalTime;

        return {
          date: item.date,
          completedCount: filterData ? filterData.count : item.totalCompleted,
          successCount: filterData ? filterData.successCount : item.successCount,
          failedCount: filterData ? filterData.failedCount : item.failedCount,
          canceledCount: filterData ? filterData.canceledCount : item.canceledCount,
          avgWaitingTime: mainTimedTaskCount > 0 ? Math.round(mainTotalWaitingTime / mainTimedTaskCount) : 0,
          avgExecutionTime: mainTimedTaskCount > 0 ? Math.round(mainTotalExecutionTime / mainTimedTaskCount) : 0,
          avgTotalTime: mainTimedTaskCount > 0 ? Math.round(mainTotalTime / mainTimedTaskCount) : 0,
          byType: Object.entries(item.byType || {}).reduce((acc, [key, val]) => {
            if (type && key !== type) return acc;
            acc[key] = {
              count: val.count,
              avgWaitingTime: val.timedTaskCount > 0 ? Math.round(val.totalWaitingTime / val.timedTaskCount) : 0,
              avgExecutionTime: val.timedTaskCount > 0 ? Math.round(val.totalExecutionTime / val.timedTaskCount) : 0,
              avgTotalTime: val.timedTaskCount > 0 ? Math.round(val.totalTime / val.timedTaskCount) : 0
            };
            return acc;
          }, {}),
          byRunnerType: Object.entries(item.byRunnerType || {}).reduce((acc, [key, val]) => {
            if (runnerType && key !== runnerType) return acc;
            acc[key] = {
              count: val.count,
              avgWaitingTime: val.timedTaskCount > 0 ? Math.round(val.totalWaitingTime / val.timedTaskCount) : 0,
              avgExecutionTime: val.timedTaskCount > 0 ? Math.round(val.totalExecutionTime / val.timedTaskCount) : 0,
              avgTotalTime: val.timedTaskCount > 0 ? Math.round(val.totalTime / val.timedTaskCount) : 0
            };
            return acc;
          }, {})
        };
      });

      return {
        range: normalizedRange,
        rangeLabel: rangeConfig.label,
        totalTasks,
        byStatus: byStatus.reduce((acc, item) => {
          acc[item.status] = Number(item.count);
          return acc;
        }, {}),
        byType: byType.reduce((acc, item) => {
          acc[item.type] = Number(item.count);
          return acc;
        }, {}),
        byRunnerType: byRunnerType.reduce((acc, item) => {
          acc[item.runnerType] = Number(item.count);
          return acc;
        }, {}),
        byTargetType: byTargetType.reduce((acc, item) => {
          acc[item.targetType] = Number(item.count);
          return acc;
        }, {}),
        recentTrend: recentTrend.map(item => ({ date: adjustDate(item.date), count: Number(item.count) })),
        recentTrendByStatus: recentTrendByStatus.map(item => ({ date: adjustDate(item.date), status: item.status, count: Number(item.count) })),
        recentTrendByType: recentTrendByType.map(item => ({ date: adjustDate(item.date), type: item.type, count: Number(item.count) })),
        durationTrend
      };
    },

    getRealtime: async ({ timezone, type, runnerType } = {}) => {
      const { Sequelize } = models.task.sequelize;
      const taskModel = models.task;
      const createdAtCol = taskModel.rawAttributes.createdAt.field;
      const dialect = taskModel.sequelize.getDialect();
      const effectiveTimezone = resolveTimezone(timezone);

      const todayStart = getTodayStart(effectiveTimezone);
      const whereToday = { createdAt: { [Sequelize.Op.gte]: todayStart } };
      if (type) {
        whereToday.type = type;
      }
      if (runnerType) {
        whereToday.runnerType = runnerType;
      }

      // 按小时提取
      const hourExtract = (() => {
        if (!effectiveTimezone || dialect === 'sqlite') {
          return dialect === 'sqlite'
            ? Sequelize.fn('strftime', '%H', Sequelize.col(createdAtCol))
            : Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM "${createdAtCol}"`));
        }
        if (dialect === 'postgres') {
          return Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM "${createdAtCol}" AT TIME ZONE '${effectiveTimezone}'`));
        }
        return Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM CONVERT_TZ("${createdAtCol}", '+00:00', '${effectiveTimezone}')`));
      })();

      // 15分钟间隔提取
      const intervalExtract = (() => {
        if (dialect === 'sqlite') {
          return Sequelize.literal(`strftime('%H', "${createdAtCol}") || ':' || printf('%02d', CAST(strftime('%M', "${createdAtCol}") AS INTEGER) / 15 * 15)`);
        }
        if (!effectiveTimezone) {
          if (dialect === 'postgres') {
            return Sequelize.literal(`TO_CHAR("${createdAtCol}", 'HH24:') || LPAD((FLOOR(EXTRACT(MINUTE FROM "${createdAtCol}") / 15) * 15)::TEXT, 2, '0')`);
          }
          return Sequelize.literal(`CONCAT(DATE_FORMAT("${createdAtCol}", '%H:'), LPAD(FLOOR(EXTRACT(MINUTE FROM "${createdAtCol}") / 15) * 15, 2, '0'))`);
        }
        if (dialect === 'postgres') {
          return Sequelize.literal(`TO_CHAR("${createdAtCol}" AT TIME ZONE '${effectiveTimezone}', 'HH24:') || LPAD((FLOOR(EXTRACT(MINUTE FROM "${createdAtCol}" AT TIME ZONE '${effectiveTimezone}') / 15) * 15)::TEXT, 2, '0')`);
        }
        return Sequelize.literal(`CONCAT(DATE_FORMAT(CONVERT_TZ("${createdAtCol}", '+00:00', '${effectiveTimezone}'), '%H:'), LPAD(FLOOR(EXTRACT(MINUTE FROM CONVERT_TZ("${createdAtCol}", '+00:00', '${effectiveTimezone}')) / 15) * 15, 2, '0'))`);
      })();

      const [totalTasks, byStatus, byType, byRunnerType, hourlyTrend, hourlyTrendByStatus, hourlyTrendByType, intervalTrend] = await Promise.all([
        taskModel.count({ where: whereToday }),
        taskModel.findAll({
          attributes: ['status', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereToday,
          group: ['status'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['type', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereToday,
          group: ['type'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['runnerType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereToday,
          group: ['runnerType'],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [hourExtract, 'hour'],
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereToday,
          group: [hourExtract],
          order: [[hourExtract, 'ASC']],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [hourExtract, 'hour'],
            'status',
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereToday,
          group: [hourExtract, 'status'],
          order: [[hourExtract, 'ASC'], ['status', 'ASC']],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [hourExtract, 'hour'],
            'type',
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereToday,
          group: [hourExtract, 'type'],
          order: [[hourExtract, 'ASC'], ['type', 'ASC']],
          raw: true
        }),
        taskModel.findAll({
          attributes: [
            [intervalExtract, 'interval'],
            [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
          ],
          where: whereToday,
          group: [intervalExtract],
          order: [[intervalExtract, 'ASC']],
          raw: true
        })
      ]);

      // SQLite的hour/interval是UTC，需按timezone偏移修正
      const adjustHour = hour => {
        if (!effectiveTimezone || dialect !== 'sqlite') return Number(hour);
        const utcTime = dayjs.utc().startOf('day').hour(Number(hour));
        return utcTime.tz(effectiveTimezone).hour();
      };

      const adjustInterval = intervalStr => {
        if (!effectiveTimezone || dialect !== 'sqlite') return intervalStr;
        const [h, m] = intervalStr.split(':').map(Number);
        const utcTime = dayjs.utc().startOf('day').hour(h).minute(m);
        return utcTime.tz(effectiveTimezone).format('HH:mm');
      };

      return {
        date: formatDate(todayStart, effectiveTimezone),
        totalTasks,
        byStatus: byStatus.reduce((acc, item) => {
          acc[item.status] = Number(item.count);
          return acc;
        }, {}),
        byType: byType.reduce((acc, item) => {
          acc[item.type] = Number(item.count);
          return acc;
        }, {}),
        byRunnerType: byRunnerType.reduce((acc, item) => {
          acc[item.runnerType] = Number(item.count);
          return acc;
        }, {}),
        hourlyTrend: hourlyTrend.map(item => ({ hour: adjustHour(item.hour), count: Number(item.count) })),
        hourlyTrendByStatus: hourlyTrendByStatus.map(item => ({
          hour: adjustHour(item.hour),
          status: item.status,
          count: Number(item.count)
        })),
        hourlyTrendByType: hourlyTrendByType.map(item => ({
          hour: adjustHour(item.hour),
          type: item.type,
          count: Number(item.count)
        })),
        intervalTrend: intervalTrend.map(item => ({ interval: adjustInterval(item.interval), count: Number(item.count) })),
        todayDuration: await (async () => {
          const statDateKey = formatDate(todayStart, effectiveTimezone);
          const todayStat = await models.taskDailyStatistics.findOne({
            where: { date: statDateKey },
            raw: true
          });

          const emptyDuration = () => ({
            completedCount: 0,
            successCount: 0,
            failedCount: 0,
            canceledCount: 0,
            avgWaitingTime: 0,
            avgExecutionTime: 0,
            avgTotalTime: 0,
            byType: {},
            byRunnerType: {}
          });

          const mapStatToDuration = baseStat => {
            if (!baseStat) return emptyDuration();
            const typeData = type && baseStat.byType && baseStat.byType[type] ? baseStat.byType[type] : null;
            const runnerTypeData = runnerType && baseStat.byRunnerType && baseStat.byRunnerType[runnerType] ? baseStat.byRunnerType[runnerType] : null;
            const filterData = runnerTypeData || typeData;
            const mainTimedTaskCount = filterData ? filterData.timedTaskCount : baseStat.timedTaskCount;
            const mainTotalWaitingTime = filterData ? filterData.totalWaitingTime : baseStat.totalWaitingTime;
            const mainTotalExecutionTime = filterData ? filterData.totalExecutionTime : baseStat.totalExecutionTime;
            const mainTotalTime = filterData ? filterData.totalTime : baseStat.totalTime;

            return {
              completedCount: filterData ? filterData.count : baseStat.totalCompleted,
              successCount: filterData ? filterData.successCount : baseStat.successCount,
              failedCount: filterData ? filterData.failedCount : baseStat.failedCount,
              canceledCount: filterData ? filterData.canceledCount : baseStat.canceledCount,
              avgWaitingTime: mainTimedTaskCount > 0 ? Math.round(mainTotalWaitingTime / mainTimedTaskCount) : 0,
              avgExecutionTime: mainTimedTaskCount > 0 ? Math.round(mainTotalExecutionTime / mainTimedTaskCount) : 0,
              avgTotalTime: mainTimedTaskCount > 0 ? Math.round(mainTotalTime / mainTimedTaskCount) : 0,
              byType: Object.entries(baseStat.byType || {}).reduce((acc, [key, val]) => {
                if (type && key !== type) return acc;
                acc[key] = {
                  count: val.count,
                  avgWaitingTime: val.timedTaskCount > 0 ? Math.round(val.totalWaitingTime / val.timedTaskCount) : 0,
                  avgExecutionTime: val.timedTaskCount > 0 ? Math.round(val.totalExecutionTime / val.timedTaskCount) : 0,
                  avgTotalTime: val.timedTaskCount > 0 ? Math.round(val.totalTime / val.timedTaskCount) : 0
                };
                return acc;
              }, {}),
              byRunnerType: Object.entries(baseStat.byRunnerType || {}).reduce((acc, [key, val]) => {
                if (runnerType && key !== runnerType) return acc;
                acc[key] = {
                  count: val.count,
                  avgWaitingTime: val.timedTaskCount > 0 ? Math.round(val.totalWaitingTime / val.timedTaskCount) : 0,
                  avgExecutionTime: val.timedTaskCount > 0 ? Math.round(val.totalExecutionTime / val.timedTaskCount) : 0,
                  avgTotalTime: val.timedTaskCount > 0 ? Math.round(val.totalTime / val.timedTaskCount) : 0
                };
                return acc;
              }, {})
            };
          };

          const aggregateDurationFromTodayTasks = async () => {
            const terminalStatuses = ['success', 'failed', 'canceled'];
            const tasks = await taskModel.findAll({
              where: {
                ...whereToday,
                status: { [Op.in]: terminalStatuses },
                completedAt: { [Op.ne]: null }
              },
              attributes: ['type', 'runnerType', 'status', 'createdAt', 'startedAt', 'completedAt'],
              raw: true
            });

            const newBucket = () => ({
              count: 0,
              successCount: 0,
              failedCount: 0,
              canceledCount: 0,
              totalWaitingTime: 0,
              totalExecutionTime: 0,
              totalTime: 0,
              timedTaskCount: 0
            });

            const bump = (bucket, task, timing) => {
              bucket.count += 1;
              if (task.status === 'success') bucket.successCount += 1;
              else if (task.status === 'failed') bucket.failedCount += 1;
              else if (task.status === 'canceled') bucket.canceledCount += 1;
              if (timing.hasTiming) {
                bucket.totalWaitingTime += timing.waitingTime;
                bucket.totalExecutionTime += timing.executionTime;
                bucket.totalTime += timing.totalTime;
                bucket.timedTaskCount += 1;
              }
            };

            const byTypeAcc = {};
            const byRunnerAcc = {};
            const main = newBucket();

            for (const task of tasks) {
              const timing = computeTaskTiming(task);
              bump(main, task, timing);
              if (!byTypeAcc[task.type]) byTypeAcc[task.type] = newBucket();
              bump(byTypeAcc[task.type], task, timing);
              const rt = task.runnerType || 'system';
              if (!byRunnerAcc[rt]) byRunnerAcc[rt] = newBucket();
              bump(byRunnerAcc[rt], task, timing);
            }

            const finalizeBuckets = acc =>
              Object.entries(acc).reduce((out, [key, b]) => {
                out[key] = {
                  count: b.count,
                  avgWaitingTime: b.timedTaskCount > 0 ? Math.round(b.totalWaitingTime / b.timedTaskCount) : 0,
                  avgExecutionTime: b.timedTaskCount > 0 ? Math.round(b.totalExecutionTime / b.timedTaskCount) : 0,
                  avgTotalTime: b.timedTaskCount > 0 ? Math.round(b.totalTime / b.timedTaskCount) : 0
                };
                return out;
              }, {});

            return {
              completedCount: main.count,
              successCount: main.successCount,
              failedCount: main.failedCount,
              canceledCount: main.canceledCount,
              avgWaitingTime: main.timedTaskCount > 0 ? Math.round(main.totalWaitingTime / main.timedTaskCount) : 0,
              avgExecutionTime: main.timedTaskCount > 0 ? Math.round(main.totalExecutionTime / main.timedTaskCount) : 0,
              avgTotalTime: main.timedTaskCount > 0 ? Math.round(main.totalTime / main.timedTaskCount) : 0,
              byType: finalizeBuckets(byTypeAcc),
              byRunnerType: finalizeBuckets(byRunnerAcc)
            };
          };

          const fromTasks = await aggregateDurationFromTodayTasks();

          const statHasAggregate = ts =>
            ts &&
            (Number(ts.totalCompleted) > 0 ||
              Number(ts.timedTaskCount) > 0 ||
              (ts.byType && Object.keys(ts.byType).length > 0) ||
              (ts.byRunnerType && Object.keys(ts.byRunnerType).length > 0));

          if (todayStat && statHasAggregate(todayStat)) {
            return mapStatToDuration(todayStat);
          }
          if (fromTasks.completedCount > 0) {
            return fromTasks;
          }
          if (todayStat) {
            return mapStatToDuration(todayStat);
          }
          return emptyDuration();
        })()
      };
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
    log,
    logWithSignature,
    callback,
    callbackWithSignature,
    executor,
    processNext,
    processSystemTask,
    waitingComplete,
    statistics
  });
});

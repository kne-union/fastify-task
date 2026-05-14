const fp = require('fastify-plugin');
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');

dayjs.extend(utc);
dayjs.extend(timezone);

/** 任务看板统计：日/小时预聚合、概览与实时（独立 service 插件，见业务插件开发指南「服务模块化」） */
module.exports = fp(async (fastify, options) => {
  const { models } = fastify[options.name];
  const { Op } = models.task.sequelize.Sequelize;

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
    const endAt = completedAt || (task.updatedAt ? new Date(task.updatedAt) : null);
    let waitingTime = 0;
    let executionTime = 0;
    let totalTime = 0;
    let hasTiming = false;
    if (createdAt && endAt && !Number.isNaN(createdAt.getTime()) && !Number.isNaN(endAt.getTime())) {
      totalTime = endAt.getTime() - createdAt.getTime();
      if (startedAt && !Number.isNaN(startedAt.getTime())) {
        waitingTime = startedAt.getTime() - createdAt.getTime();
        executionTime = endAt.getTime() - startedAt.getTime();
      } else {
        executionTime = totalTime;
      }
      hasTiming = totalTime > 0;
    }
    return { waitingTime, executionTime, totalTime, hasTiming };
  };

  /** MySQL BIGINT / 部分驱动下字段会以字符串回读，与 number 做 + 会变成拼接字符串，必须把累加基数规范成数字 */
  const n = v => {
    const x = Number(v);
    return Number.isFinite(x) ? x : 0;
  };

  const parseJsonField = val => {
    if (val == null) return {};
    if (typeof val === 'object' && !Array.isArray(val)) return val;
    if (typeof val === 'string') {
      try {
        const o = JSON.parse(val);
        return o && typeof o === 'object' && !Array.isArray(o) ? o : {};
      } catch {
        return {};
      }
    }
    return {};
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
              count: 0,
              successCount: 0,
              failedCount: 0,
              canceledCount: 0,
              totalWaitingTime: 0,
              totalExecutionTime: 0,
              totalTime: 0,
              timedTaskCount: 0
            };
            return Object.assign({}, obj, {
              [key]: {
                count: n(current.count) + (increment.count || 0),
                successCount: n(current.successCount) + (increment.successCount || 0),
                failedCount: n(current.failedCount) + (increment.failedCount || 0),
                canceledCount: n(current.canceledCount) + (increment.canceledCount || 0),
                totalWaitingTime: n(current.totalWaitingTime) + (increment.totalWaitingTime || 0),
                totalExecutionTime: n(current.totalExecutionTime) + (increment.totalExecutionTime || 0),
                totalTime: n(current.totalTime) + (increment.totalTime || 0),
                timedTaskCount: n(current.timedTaskCount) + (increment.timedTaskCount || 0)
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

          const baseByType = parseJsonField(stat.byType);
          const baseByRunnerType = parseJsonField(stat.byRunnerType);

          await stat.update({
            totalCompleted: n(stat.totalCompleted) + 1,
            successCount: n(stat.successCount) + (status === 'success' ? 1 : 0),
            failedCount: n(stat.failedCount) + (status === 'failed' ? 1 : 0),
            canceledCount: n(stat.canceledCount) + (status === 'canceled' ? 1 : 0),
            totalWaitingTime: n(stat.totalWaitingTime) + (hasTiming ? waitingTime : 0),
            totalExecutionTime: n(stat.totalExecutionTime) + (hasTiming ? executionTime : 0),
            totalTime: n(stat.totalTime) + (hasTiming ? totalTime : 0),
            timedTaskCount: n(stat.timedTaskCount) + (hasTiming ? 1 : 0),
            byType: mergeKey(baseByType, task.type, typeIncrement),
            byRunnerType: mergeKey(baseByRunnerType, task.runnerType, typeIncrement)
          });
        }
      } catch (e) {
        fastify.log.error(`更新每日统计失败: ${e.message}`);
      }
    })();
  };

  /**
   * 按任务表 completedAt 重算某一 UTC 整点桶的完成数（不写实时增量，供定时任务调用）。
   * 未传 bucketStartUtc 时默认聚合「上一完整 UTC 小时」。
   */
  const syncHourlyStatisticsFromTasks = async ({ bucketStartUtc } = {}) => {
    const taskModel = models.task;
    const { Sequelize } = taskModel.sequelize;
    const bucket = bucketStartUtc != null ? dayjs.utc(bucketStartUtc).startOf('hour') : dayjs.utc().subtract(1, 'hour').startOf('hour');
    const hourStart = bucket.toDate();
    const hourEnd = bucket.add(1, 'hour').toDate();

    try {
      const rows = await taskModel.findAll({
        where: {
          status: { [Sequelize.Op.in]: ['success', 'failed', 'canceled'] },
          completedAt: {
            [Sequelize.Op.gte]: hourStart,
            [Sequelize.Op.lt]: hourEnd
          }
        },
        raw: true
      });

      const map = new Map();
      for (const t of rows) {
        const typ = t.type;
        const runnerTypeKey = t.runnerType || 'manual';
        const key = `${typ}\0${runnerTypeKey}`;
        if (!map.has(key)) {
          map.set(key, {
            type: typ,
            runnerType: runnerTypeKey,
            totalCompleted: 0,
            successCount: 0,
            failedCount: 0,
            canceledCount: 0
          });
        }
        const acc = map.get(key);
        acc.totalCompleted += 1;
        if (t.status === 'success') acc.successCount += 1;
        else if (t.status === 'failed') acc.failedCount += 1;
        else if (t.status === 'canceled') acc.canceledCount += 1;
      }

      await models.taskHourlyStatistics.destroy({
        where: { bucketStartUtc: hourStart }
      });

      for (const agg of map.values()) {
        await models.taskHourlyStatistics.create({
          bucketStartUtc: hourStart,
          type: agg.type,
          runnerType: agg.runnerType,
          totalCompleted: agg.totalCompleted,
          successCount: agg.successCount,
          failedCount: agg.failedCount,
          canceledCount: agg.canceledCount
        });
      }
    } catch (e) {
      fastify.log.error(`同步小时统计失败: ${e.message}`);
    }
  };

  const flushCompletionStatistics = task => {
    updateDailyStatistics(task);
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
      // 与「按创建日 + 用户时区」的聚合一致：用同一时区做日历减法，避免 Date#setDate 与服务器本地时区混用
      const rangeStartDayjs = dayjs().tz(effectiveTimezone).startOf('day').subtract(rangeConfig.days, 'day');
      const startDate = rangeStartDayjs.toDate();
      const minStatDateStr = rangeStartDayjs.format('YYYY-MM-DD');

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
          attributes: [[dateFn(createdAtCol), 'date'], 'status', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereRange,
          group: [dateFn(createdAtCol), 'status'],
          order: [[dateFn(createdAtCol), 'ASC']],
          raw: true
        }),
        taskModel.findAll({
          attributes: [[dateFn(createdAtCol), 'date'], 'type', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
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
        where: { date: { [Sequelize.Op.gte]: minStatDateStr } },
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

      // 时段完成活跃度：按任务表 completedAt + 客户端时区聚合（不依赖小时预聚合表，避免无 cron 数据时空白）
      const completedAtCol = taskModel.rawAttributes.completedAt?.field || 'completed_at';
      const hourExtractCompleted = (() => {
        if (!effectiveTimezone || dialect === 'sqlite') {
          return dialect === 'sqlite'
            ? Sequelize.fn('strftime', '%H', Sequelize.col(completedAtCol))
            : Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM "${completedAtCol}"`));
        }
        if (dialect === 'postgres') {
          return Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM "${completedAtCol}" AT TIME ZONE '${effectiveTimezone}'`));
        }
        return Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM CONVERT_TZ("${completedAtCol}", '+00:00', '${effectiveTimezone}')`));
      })();

      const completedRangeStart = rangeStartDayjs.toDate();
      const completedRangeEnd = dayjs().tz(effectiveTimezone).endOf('day').toDate();
      const whereHourlyCompletion = {
        completedAt: {
          [Sequelize.Op.gte]: completedRangeStart,
          [Sequelize.Op.lte]: completedRangeEnd
        },
        status: { [Sequelize.Op.in]: ['success', 'failed', 'canceled'] }
      };
      if (type) {
        whereHourlyCompletion.type = type;
      }
      if (runnerType) {
        whereHourlyCompletion.runnerType = runnerType;
      }

      const adjustHourForCompletion = hour => {
        if (!effectiveTimezone || dialect !== 'sqlite') return Number(hour);
        const utcTime = dayjs.utc().startOf('day').hour(Number(hour));
        return utcTime.tz(effectiveTimezone).hour();
      };

      const hourlyCompletionRows = await taskModel.findAll({
        attributes: [
          [dateFn(completedAtCol), 'date'],
          [hourExtractCompleted, 'hour'],
          'type',
          'runnerType',
          'status',
          [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']
        ],
        where: whereHourlyCompletion,
        group: [dateFn(completedAtCol), hourExtractCompleted, 'type', 'runnerType', 'status'],
        order: [
          [dateFn(completedAtCol), 'ASC'],
          [hourExtractCompleted, 'ASC']
        ],
        raw: true
      });

      const hourlyBucketMap = new Map();
      for (const row of hourlyCompletionRows) {
        const dateStr = adjustDate(row.date);
        const hourNum = adjustHourForCompletion(row.hour);
        const typ = row.type;
        const rt = row.runnerType || 'manual';
        const key = `${dateStr}\0${hourNum}\0${typ}\0${rt}`;
        if (!hourlyBucketMap.has(key)) {
          hourlyBucketMap.set(key, {
            date: dateStr,
            hour: hourNum,
            type: typ,
            runnerType: rt,
            totalCompleted: 0,
            successCount: 0,
            failedCount: 0,
            canceledCount: 0
          });
        }
        const b = hourlyBucketMap.get(key);
        const c = Number(row.count) || 0;
        b.totalCompleted += c;
        if (row.status === 'success') b.successCount += c;
        else if (row.status === 'failed') b.failedCount += c;
        else if (row.status === 'canceled') b.canceledCount += c;
      }
      const hourlyCompletionTrend = Array.from(hourlyBucketMap.values()).sort((a, b) =>
        a.date !== b.date
          ? a.date.localeCompare(b.date)
          : a.hour !== b.hour
            ? a.hour - b.hour
            : `${a.type}:${a.runnerType}`.localeCompare(`${b.type}:${b.runnerType}`)
      );

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
        durationTrend,
        hourlyCompletionTrend
      };
    },

    getRealtime: async ({ timezone, type, runnerType } = {}) => {
      const { Sequelize } = models.task.sequelize;
      const taskModel = models.task;
      const createdAtCol = taskModel.rawAttributes.createdAt.field;
      const dialect = taskModel.sequelize.getDialect();
      const effectiveTimezone = resolveTimezone(timezone);

      const todayStart = getTodayStart(effectiveTimezone);
      const todayEnd = effectiveTimezone
        ? dayjs().tz(effectiveTimezone).endOf('day').toDate()
        : dayjs().endOf('day').toDate();
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
          return dialect === 'sqlite' ? Sequelize.fn('strftime', '%H', Sequelize.col(createdAtCol)) : Sequelize.fn('EXTRACT', Sequelize.literal(`HOUR FROM "${createdAtCol}"`));
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
        return Sequelize.literal(
          `CONCAT(DATE_FORMAT(CONVERT_TZ("${createdAtCol}", '+00:00', '${effectiveTimezone}'), '%H:'), LPAD(FLOOR(EXTRACT(MINUTE FROM CONVERT_TZ("${createdAtCol}", '+00:00', '${effectiveTimezone}')) / 15) * 15, 2, '0'))`
        );
      })();

      const [totalTasks, byStatus, byType, byRunnerType, pendingByRunnerType, nonPendingByRunnerType, hourlyTrend, hourlyTrendByStatus, hourlyTrendByType, intervalTrend] = await Promise.all([
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
          attributes: ['runnerType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: { ...whereToday, status: 'pending' },
          group: ['runnerType'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['runnerType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: { ...whereToday, status: { [Sequelize.Op.ne]: 'pending' } },
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
          attributes: [[hourExtract, 'hour'], 'status', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereToday,
          group: [hourExtract, 'status'],
          order: [
            [hourExtract, 'ASC'],
            ['status', 'ASC']
          ],
          raw: true
        }),
        taskModel.findAll({
          attributes: [[hourExtract, 'hour'], 'type', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: whereToday,
          group: [hourExtract, 'type'],
          order: [
            [hourExtract, 'ASC'],
            ['type', 'ASC']
          ],
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

      const byRunnerTypeMap = byRunnerType.reduce((acc, item) => {
        acc[item.runnerType] = Number(item.count);
        return acc;
      }, {});
      const pendingByRunnerTypeMap = pendingByRunnerType.reduce((acc, item) => {
        acc[item.runnerType] = Number(item.count);
        return acc;
      }, {});
      const nonPendingByRunnerTypeMap = nonPendingByRunnerType.reduce((acc, item) => {
        acc[item.runnerType] = Number(item.count);
        return acc;
      }, {});

      /** 看板「等待操作」：waiting + 手动队列 pending（与 components-admin 约定字段 waitingByRunnerType 对齐） */
      const waitingByRunnerWhere = {
        [Op.or]: [{ status: 'waiting' }, { runnerType: 'manual', status: 'pending' }]
      };
      if (type) {
        waitingByRunnerWhere.type = type;
      }
      if (runnerType) {
        waitingByRunnerWhere.runnerType = runnerType;
      }

      /** 看板「当日完成」：完成时间落在当日（用户时区）且终态，按 runnerType 分组 → completedToday */
      const completedTodayWhere = {
        completedAt: {
          [Op.gte]: todayStart,
          [Op.lte]: todayEnd
        },
        status: { [Op.in]: ['success', 'failed', 'canceled'] }
      };
      if (type) {
        completedTodayWhere.type = type;
      }
      if (runnerType) {
        completedTodayWhere.runnerType = runnerType;
      }

      const [waitingByRunnerTypeRows, completedTodayRows] = await Promise.all([
        taskModel.findAll({
          attributes: ['runnerType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: waitingByRunnerWhere,
          group: ['runnerType'],
          raw: true
        }),
        taskModel.findAll({
          attributes: ['runnerType', [Sequelize.fn('COUNT', Sequelize.col('id')), 'count']],
          where: completedTodayWhere,
          group: ['runnerType'],
          raw: true
        })
      ]);

      const rowsToRunnerMap = rows =>
        rows.reduce((acc, item) => {
          acc[item.runnerType] = Number(item.count) || 0;
          return acc;
        }, {});

      const waitingByRunnerType = rowsToRunnerMap(waitingByRunnerTypeRows);
      const completedToday = rowsToRunnerMap(completedTodayRows);

      const [waitingQueueRows, completedTodayTimingRows] = await Promise.all([
        taskModel.findAll({
          where: waitingByRunnerWhere,
          attributes: ['runnerType', 'createdAt'],
          raw: true
        }),
        taskModel.findAll({
          where: completedTodayWhere,
          attributes: ['runnerType', 'createdAt', 'completedAt'],
          raw: true
        })
      ]);

      const nowMs = Date.now();
      const waitingQueueMaxWaitMsByRunnerType = {};
      for (const row of waitingQueueRows) {
        const rt = row.runnerType;
        const created = row.createdAt ? new Date(row.createdAt).getTime() : nowMs;
        const waitMs = Math.max(0, nowMs - created);
        const prev = waitingQueueMaxWaitMsByRunnerType[rt] || 0;
        waitingQueueMaxWaitMsByRunnerType[rt] = Math.max(prev, waitMs);
      }

      const completedTodayTotalDurationMsByRunnerType = {};
      for (const row of completedTodayTimingRows) {
        const rt = row.runnerType || 'manual';
        const ca = row.completedAt ? new Date(row.completedAt).getTime() : NaN;
        const cr = row.createdAt ? new Date(row.createdAt).getTime() : NaN;
        if (!Number.isFinite(ca) || !Number.isFinite(cr)) continue;
        const dur = Math.max(0, ca - cr);
        completedTodayTotalDurationMsByRunnerType[rt] = (completedTodayTotalDurationMsByRunnerType[rt] || 0) + dur;
      }

      const runnerTypeStatsKeys = new Set([...Object.keys(byRunnerTypeMap), ...Object.keys(pendingByRunnerTypeMap), ...Object.keys(nonPendingByRunnerTypeMap)]);
      const runnerTypeStats = {};
      for (const k of runnerTypeStatsKeys) {
        runnerTypeStats[k] = {
          total: Number(byRunnerTypeMap[k]) || 0,
          pending: Number(pendingByRunnerTypeMap[k]) || 0,
          executed: Number(nonPendingByRunnerTypeMap[k]) || 0
        };
      }

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
        byRunnerType: byRunnerTypeMap,
        pendingByRunnerType: pendingByRunnerTypeMap,
        waitingByRunnerType,
        completedToday,
        waitingQueueMaxWaitMsByRunnerType,
        completedTodayTotalDurationMsByRunnerType,
        runnerTypeStats,
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
            byRunnerType: {},
            /** 看板「按类型 × 执行方式」：仅当日任务行回算时有值；日统计 JSON 未存交叉维度时为空 */
            byTypeByRunnerType: { manual: {}, system: {} }
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
              }, {}),
              byTypeByRunnerType: { manual: {}, system: {} }
            };
          };

          const aggregateDurationFromTodayTasks = async () => {
            const terminalStatuses = ['success', 'failed', 'canceled'];
            const tasks = await taskModel.findAll({
              where: {
                ...whereToday,
                status: { [Op.in]: terminalStatuses }
              },
              attributes: ['type', 'runnerType', 'status', 'createdAt', 'startedAt', 'completedAt', 'updatedAt'],
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
            const byTypeManualAcc = {};
            const byTypeSystemAcc = {};
            const byRunnerAcc = {};
            const main = newBucket();

            for (const task of tasks) {
              const timing = computeTaskTiming(task);
              bump(main, task, timing);
              if (!byTypeAcc[task.type]) byTypeAcc[task.type] = newBucket();
              bump(byTypeAcc[task.type], task, timing);
              const splitAcc = task.runnerType === 'manual' ? byTypeManualAcc : byTypeSystemAcc;
              if (!splitAcc[task.type]) splitAcc[task.type] = newBucket();
              bump(splitAcc[task.type], task, timing);
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
              byRunnerType: finalizeBuckets(byRunnerAcc),
              byTypeByRunnerType: {
                manual: finalizeBuckets(byTypeManualAcc),
                system: finalizeBuckets(byTypeSystemAcc)
              }
            };
          };

          const fromTasks = await aggregateDurationFromTodayTasks();

          const statHasAggregate = ts => ts && (Number(ts.totalCompleted) > 0 || Number(ts.timedTaskCount) > 0 || (ts.byType && Object.keys(ts.byType).length > 0) || (ts.byRunnerType && Object.keys(ts.byRunnerType).length > 0));

          // 实时接口与当日任务表一致：优先按任务行回算；日统计可能滞后或仅部分类型已写入
          if (fromTasks.completedCount > 0) {
            return fromTasks;
          }
          if (todayStat && statHasAggregate(todayStat)) {
            return mapStatToDuration(todayStat);
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
    flushCompletionStatistics,
    syncHourlyStatisticsFromTasks,
    statistics
  });
});

module.exports = ({ DataTypes, definePrimaryType, options }) => {
  return {
    model: {
      date: {
        type: DataTypes.DATEONLY,
        comment: '统计日期(按完成日期)'
      },
      totalCompleted: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
        comment: '当天完成任务总数'
      },
      successCount: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
        comment: '成功数'
      },
      failedCount: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
        comment: '失败数'
      },
      canceledCount: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
        comment: '取消数'
      },
      totalWaitingTime: {
        type: DataTypes.BIGINT,
        defaultValue: 0,
        comment: '总等待时间(毫秒)'
      },
      totalExecutionTime: {
        type: DataTypes.BIGINT,
        defaultValue: 0,
        comment: '总执行时间(毫秒)'
      },
      totalTime: {
        type: DataTypes.BIGINT,
        defaultValue: 0,
        comment: '总耗时(毫秒, completedAt-createdAt)'
      },
      timedTaskCount: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
        comment: '有耗时数据的任务数'
      },
      byType: {
        type: DataTypes.JSON,
        defaultValue: {},
        comment: '按任务类型分组统计'
      },
      byRunnerType: {
        type: DataTypes.JSON,
        defaultValue: {},
        comment: '按执行者类型分组统计'
      }
    },
    options: {
      comment: '任务每日统计',
      indexes: [
        { fields: ['date'], unique: true }
      ]
    }
  };
};

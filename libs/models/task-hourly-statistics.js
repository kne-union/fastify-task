module.exports = ({ DataTypes }) => {
  return {
    model: {
      bucketStartUtc: {
        type: DataTypes.DATE,
        allowNull: false,
        comment: 'UTC 整点桶起始时间（按任务完成时刻归入该小时，与客户端时区无关）'
      },
      type: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '任务类型'
      },
      runnerType: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '执行方式 manual / system'
      },
      totalCompleted: {
        type: DataTypes.INTEGER,
        defaultValue: 0,
        comment: '该桶内完成任务数'
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
      }
    },
    options: {
      comment: '任务按 UTC 小时 + 类型 + 执行方式的完成数预聚合（由定时任务从任务表重算，读接口按客户端时区换算展示）',
      indexes: [
        { unique: true, fields: ['bucket_start_utc', 'type', 'runner_type'] },
        { fields: ['bucket_start_utc'] }
      ]
    }
  };
};

module.exports = ({ DataTypes, definePrimaryType, options }) => {
  return {
    model: {
      type: {
        type: DataTypes.STRING,
        comment: '任务类型'
      },
      scriptName: {
        type: DataTypes.STRING,
        comment: '任务脚本名称'
      },
      targetId: definePrimaryType('targetId', {
        comment: '任务目标对象id',
        allowNull: false
      }),
      targetType: {
        type: DataTypes.STRING,
        comment: '任务目标对象类型',
        allowNull: false
      },
      runnerType: {
        type: DataTypes.ENUM('manual', 'system'),
        comment: '任务执行者类型: manual为手动执行, system为系统自动执行',
        defaultValue: 'manual',
        allowNull: false
      },
      startTime: {
        type: DataTypes.DATE,
        comment: '任务最早执行时间',
        defaultValue: new Date()
      },
      completedAt: {
        type: DataTypes.DATE,
        comment: '任务完成时间'
      },
      input: {
        type: DataTypes.JSON,
        comment: '输入数据'
      },
      output: {
        type: DataTypes.JSON,
        comment: '输出数据'
      },
      error: {
        type: DataTypes.JSON,
        comment: '错误信息'
      },
      status: {
        type: DataTypes.ENUM('pending', 'running', 'waiting', 'success', 'failed', 'canceled'),
        comment: '任务状态',
        defaultValue: 'pending'
      },
      context: {
        type: DataTypes.JSON,
        comment: '上下文信息',
        defaultValue: {}
      },
      pollResults: {
        type: DataTypes.JSON,
        comment: '轮询执行结果',
        defaultValue: []
      },
      pollCount: {
        type: DataTypes.INTEGER,
        comment: '轮询次数',
        defaultValue: 0
      },
      progress: {
        type: DataTypes.INTEGER,
        comment: '任务进度(0-100)',
        defaultValue: 0
      },
      msg: {
        type: DataTypes.TEXT,
        comment: '任务消息或错误信息'
      },
      options: {
        type: DataTypes.JSON,
        comment: '任务扩展选项'
      }
    },
    associate: ({ task }) => {
      task.belongsTo(options.getUserModel(), {
        foreignKey: 'userId',
        as: 'createdUser',
        comment: '创建人(为空时为系统创建)'
      });
      task.belongsTo(options.getUserModel(), {
        foreignKey: 'completedUserId',
        as: 'completedUser',
        comment: '任务完成人'
      });
    },
    options: {
      comment: '任务'
    }
  };
};

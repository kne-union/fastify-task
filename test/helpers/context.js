const sinon = require('sinon');
const path = require('node:path');

const createTestContext = () => {
  let fastify;
  let taskData = [];
  let taskIdCounter = 1;

  const mockUserModel = {
    findByPk: sinon.stub(),
    findOne: sinon.stub()
  };

  const mockAuthenticate = {
    user: async () => {},
    admin: async () => {},
    read: async () => {},
    write: async () => {}
  };

  const getValueByPath = (obj, path) => {
    return path.split('.').reduce((acc, key) => (acc == null ? acc : acc[key]), obj);
  };

  const matchOperatorCondition = (fieldValue, condition, Op) => {
    if (!condition || typeof condition !== 'object' || Array.isArray(condition)) {
      return fieldValue === condition;
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.between)) {
      const [start, end] = condition[Op.between] || [];
      return fieldValue >= start && fieldValue <= end;
    }
    const rangeChecks = [];
    if (Object.prototype.hasOwnProperty.call(condition, Op.gte)) rangeChecks.push(fieldValue >= condition[Op.gte]);
    if (Object.prototype.hasOwnProperty.call(condition, Op.gt)) rangeChecks.push(fieldValue > condition[Op.gt]);
    if (Object.prototype.hasOwnProperty.call(condition, Op.lte)) rangeChecks.push(fieldValue <= condition[Op.lte]);
    if (Object.prototype.hasOwnProperty.call(condition, Op.lt)) rangeChecks.push(fieldValue < condition[Op.lt]);
    if (rangeChecks.length) return rangeChecks.every(Boolean);
    if (Object.prototype.hasOwnProperty.call(condition, Op.in)) {
      const list = condition[Op.in] || [];
      return list.includes(fieldValue);
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.ne)) {
      return fieldValue !== condition[Op.ne];
    }
    if (Object.prototype.hasOwnProperty.call(condition, Op.like)) {
      const pattern = String(condition[Op.like] || '');
      const normalized = pattern.replaceAll('%', '');
      return String(fieldValue || '').includes(normalized);
    }
    return fieldValue === condition;
  };

  const matchWhere = (item, where = {}, Op) => {
    if (!where || typeof where !== 'object') return true;
    const orBranches = Op.or != null ? where[Op.or] : undefined;
    const rest = { ...where };
    if (Op.or != null && Object.prototype.hasOwnProperty.call(rest, Op.or)) {
      delete rest[Op.or];
    }

    const matchRest = Object.entries(rest).every(([key, condition]) => {
      const fieldValue = getValueByPath(item, key);
      return matchOperatorCondition(fieldValue, condition, Op);
    });
    if (orBranches == null) return matchRest;

    const branches = Array.isArray(orBranches) ? orBranches : [orBranches];
    const matchOr = branches.some(branch => {
      if (branch && typeof branch === 'object' && !Array.isArray(branch)) {
        return Object.entries(branch).every(([key, condition]) => {
          const fieldValue = getValueByPath(item, key);
          return matchOperatorCondition(fieldValue, condition, Op);
        });
      }
      return false;
    });

    return matchRest && matchOr;
  };

  const createMockTaskModel = Op => {
    return {
      create: async data => {
        const task = {
          id: `task-${taskIdCounter++}`,
          ...data,
          status: data.status || 'pending',
          progress: data.progress || 0,
          pollCount: data.pollCount || 0,
          pollResults: data.pollResults || [],
          context: data.context || {},
          options: data.options || {},
          priority: data.priority || 0,
          parentTaskId: data.parentTaskId || null,
          retryCount: data.retryCount || 0,
          maxRetries: data.maxRetries || 0,
          completedUserId: data.completedUserId || null,
          input: data.input !== undefined ? data.input : null,
          output: data.output !== undefined ? data.output : null,
          createdAt: data.createdAt || new Date(),
          updatedAt: data.updatedAt || new Date(),
          update: async function (updateData) {
            Object.assign(this, updateData);
            this.updatedAt = new Date();
            return this;
          },
          reload: async function () {
            return this;
          }
        };
        taskData.push(task);
        return task;
      },
      findByPk: async id => {
        return taskData.find(t => t.id === id) || null;
      },
      findAll: async ({ where, limit, attributes, group, order, raw } = {}) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
        }

        // Handle grouped queries
        if (group) {
          const groupFields = Array.isArray(group) ? group : [group];
          const groupKeys = groupFields
            .map(g => {
              if (g && typeof g === 'object' && g.fn === 'DATE') return 'createdAt';
              if (typeof g === 'string') return g;
              return null;
            })
            .filter(Boolean);

          const groups = {};
          results.forEach(t => {
            const key = groupKeys
              .map(k => {
                const val = t[k];
                return val instanceof Date ? `${val.getFullYear()}-${String(val.getMonth() + 1).padStart(2, '0')}-${String(val.getDate()).padStart(2, '0')}` : String(val);
              })
              .join('|');

            if (!groups[key]) {
              groups[key] = { items: [], keyValues: {} };
              groupKeys.forEach(k => {
                const val = t[k];
                groups[key].keyValues[k === 'createdAt' ? 'date' : k] = val instanceof Date ? `${val.getFullYear()}-${String(val.getMonth() + 1).padStart(2, '0')}-${String(val.getDate()).padStart(2, '0')}` : val;
              });
            }
            groups[key].items.push(t);
          });

          return Object.values(groups).map(g => {
            const row = { ...g.keyValues };
            if (attributes) {
              attributes.forEach(attr => {
                if (typeof attr === 'string') {
                  row[attr] = g.items[0]?.[attr];
                } else if (Array.isArray(attr) && attr.length === 2) {
                  const [expr, alias] = attr;
                  if (expr && typeof expr === 'object' && expr.fn === 'COUNT') {
                    row[alias] = g.items.length;
                  }
                }
              });
            }
            return row;
          });
        }

        // Handle attribute projection for non-grouped queries
        if (attributes && !group) {
          const attrKeys = attributes.map(a => (typeof a === 'string' ? a : Array.isArray(a) ? a[1] : null)).filter(Boolean);
          if (raw && attrKeys.length > 0) {
            results = results.map(t => {
              const row = {};
              attrKeys.forEach(k => {
                row[k] = t[k];
              });
              return row;
            });
          }
        }

        if (order && Array.isArray(order)) {
          results = [...results].sort((a, b) => {
            for (const [key, direction] of order) {
              const aVal = a[key];
              const bVal = b[key];
              if (aVal < bVal) return direction === 'DESC' ? 1 : -1;
              if (aVal > bVal) return direction === 'DESC' ? -1 : 1;
            }
            return 0;
          });
        }

        return results.slice(0, limit);
      },
      findAndCountAll: async ({ where, offset, limit, order }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
        }

        if (order && Array.isArray(order)) {
          results = [...results].sort((a, b) => {
            for (const [key, direction] of order) {
              const aVal = a[key];
              const bVal = b[key];
              if (aVal < bVal) return direction === 'DESC' ? 1 : -1;
              if (aVal > bVal) return direction === 'DESC' ? -1 : 1;
            }
            return 0;
          });
        }

        return {
          rows: results.slice(offset, offset + limit),
          count: results.length
        };
      },
      count: async ({ where }) => {
        let results = taskData;
        if (where) {
          results = taskData.filter(t => matchWhere(t, where, Op));
        }
        return results.length;
      },
      update: async (updateData, { where }) => {
        let count = 0;
        taskData.forEach(t => {
          if (matchWhere(t, where, Op)) {
            Object.assign(t, updateData);
            count++;
          }
        });
        return [count];
      },
      rawAttributes: {
        createdAt: { field: 'created_at' },
        completedAt: { field: 'completed_at' }
      },
      sequelize: {
        Sequelize: {
          Op,
          fn: sinon.stub().callsFake((fn, ...args) => ({ fn, args })),
          col: sinon.stub().callsFake(col => ({ col })),
          literal: sinon.stub().callsFake(lit => ({ literal: lit }))
        },
        getDialect: sinon.stub().returns('sqlite')
      }
    };
  };

  const createFastify = async (options = {}) => {
    const app = require('fastify')();
    const Op = {
      in: Symbol('in'),
      lte: Symbol('lte'),
      gte: Symbol('gte'),
      lt: Symbol('lt'),
      gt: Symbol('gt'),
      between: Symbol('between'),
      ne: Symbol('ne'),
      like: Symbol('like'),
      or: Symbol('or')
    };

    const mockTaskModel = createMockTaskModel(Op);

    // 模拟 sequelize
    app.decorate('sequelize', {
      Sequelize: {
        Op,
        fn: sinon.stub().callsFake((fn, ...args) => ({ fn, args })),
        col: sinon.stub().callsFake(col => ({ col })),
        literal: sinon.stub().callsFake(lit => ({ literal: lit }))
      },
      models: { task: mockTaskModel }
    });

    // 模拟 account
    app.decorate('account', {
      models: { user: mockUserModel },
      authenticate: mockAuthenticate
    });

    // 模拟 cron
    app.decorate('cron', {
      createJob: sinon.stub()
    });

    // 模拟 @kne/fastify-statistics
    app.decorate('taskStatistics', {
      services: {
        collect: sinon.stub().resolves(),
        query: sinon.stub().resolves({ channelMetas: {}, list: [] }),
        channelMeta: {
          list: sinon.stub().resolves([{ channel: 'test-type' }])
        },
        sseStream: {
          send: sinon.stub().resolves()
        }
      }
    });

    // 模拟 log（Fastify内置log，不能重新decorate，用sinon替换方法）
    const originalLog = app.log;
    const mockLog = {
      info: sinon.stub(),
      error: sinon.stub(),
      warn: sinon.stub()
    };
    app.log = mockLog;

    // 创建 task 命名空间
    const taskOptions = {
      dbTableNamePrefix: 't_',
      prefix: '/api/task',
      name: 'task',
      limit: 10,
      dir: path.resolve(__dirname, '../tasks'),
      cronTime: null,
      scriptName: 'index',
      maxPollTimes: 20,
      pollInterval: 100,
      task: {
        'test-type': async ({ task, result }) => {
          return result;
        },
        'polling-type': async ({ task, result }) => {
          return result;
        },
        'next-type': async ({ task, result, context }) => {
          return result;
        },
        'progress-type': async ({ task, result }) => {
          return result;
        },
        'fail-type': async ({ task, result }) => {
          return result;
        },
        'hang-type': async ({ task, result }) => {
          return result;
        },
        'log-type': async ({ task, result }) => {
          return result;
        },
        'polling-fail-type': async ({ task, result }) => {
          return result;
        },
        'polling-pending-type': async ({ task, result }) => {
          return result;
        }
      },
      getUserModel: () => mockUserModel,
      getAuthenticate: () => [mockAuthenticate.user, mockAuthenticate.admin],
      ...options
    };

    // 初始化 dirs：与 index.js 保持一致
    if (!taskOptions.dirs) {
      taskOptions.dirs = [taskOptions.dir];
    } else if (!taskOptions.dirs.includes(taskOptions.dir)) {
      taskOptions.dirs = [taskOptions.dir, ...taskOptions.dirs];
    }

    app.decorate('task', {
      options: taskOptions,
      models: { task: mockTaskModel },
      services: {},
      controllers: {}
    });

    // 加载 services
    const serviceModule = require('../../libs/services/main');
    await serviceModule(app, taskOptions);

    // 加载 statistics service
    const statisticsServiceModule = require('../../libs/services/statistics');
    await statisticsServiceModule(app, taskOptions);

    // 加载 controllers
    const controllerModule = require('../../libs/controllers/main');
    await controllerModule(app, taskOptions);

    // 加载 statistics controller
    const statisticsModule = require('../../libs/controllers/statistics');
    await statisticsModule(app, taskOptions);

    return app;
  };

  const reset = () => {
    taskData = [];
    taskIdCounter = 1;
  };

  const restore = () => {
    sinon.restore();
  };

  return { createFastify, reset, restore };
};

module.exports = { createTestContext };

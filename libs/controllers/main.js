const fp = require('fastify-plugin');
const { any, success, empty, TASK_STATUS, RUNNER_TYPE, task, taskList } = require('../schema/response');

const TAG = '任务-任务管理';
const CALLBACK_TAG = '任务-外部回调';
const SIGNATURE_HINT = '任务 context.secret 存在时校验 HMAC-SHA256 签名（密钥为 secret，内容为 `${id}|${JSON 数据}`），不通过报错；未设置 secret 时不校验';
const timeRange = name => ({
  type: 'object',
  properties: {
    startTime: {
      type: 'string',
      description: `${name}起始时间（含），可被 Date 解析的时间字符串`
    },
    endTime: {
      type: 'string',
      description: `${name}结束时间（含），可被 Date 解析的时间字符串`
    }
  },
  description: `${name}区间查找`
});

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];
  // #14: 新增创建任务的 REST 接口
  fastify.post(
    `${options.prefix}/create`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        tags: [TAG],
        summary: '创建任务',
        description: '创建一条 pending 状态的任务，到达开始时间后由定时器按优先级调度执行（system）或等待人工完成（manual）；type 必须是已注册的任务类型，返回新任务 ID',
        body: {
          type: 'object',
          properties: {
            type: {
              type: 'string',
              description: '任务类型'
            },
            targetId: {
              type: 'string',
              description: '目标对象ID'
            },
            targetType: {
              type: 'string',
              description: '目标对象类型'
            },
            context: {
              description: '执行上下文，任务执行时可读取；其中 secret 用于外部回调签名校验'
            },
            options: {
              description: '任务扩展选项'
            },
            input: {
              type: 'object',
              description: '输入数据'
            },
            runnerType: {
              type: 'string',
              enum: ['manual', 'system'],
              description: `执行者类型，${RUNNER_TYPE}，默认 manual`
            },
            delay: {
              type: 'number',
              description: '延迟执行秒数，必须为非负数，默认 0 立即可执行'
            },
            scriptName: {
              type: 'string',
              description: '任务脚本名称（不含 .js），只能包含字母、数字、下划线或中划线；不传使用任务类型声明的脚本，默认 index'
            },
            priority: {
              type: 'number',
              description: '优先级，数值越大越优先，默认0'
            },
            parentTaskId: {
              type: 'string',
              description: '父任务ID，用于任务依赖'
            },
            maxRetries: {
              type: 'number',
              description: '最大自动重试次数，默认0'
            },
            timeout: {
              type: 'number',
              description: '任务超时时间（毫秒），0表示不超时，默认3600000ms'
            }
          },
          required: ['type', 'targetId', 'targetType']
        },
        response: success('创建成功', {
          type: 'object',
          additionalProperties: true,
          properties: { id: any('新任务 ID') }
        })
      }
    },
    async request => {
      const task = await services.create(Object.assign({}, request.body, { userId: request.userInfo?.id }));
      return { id: task.id };
    }
  );

  fastify.get(
    `${options.prefix}/list`,
    {
      onRequest: options.getAuthenticate('read'),
      schema: {
        tags: [TAG],
        summary: '获取任务列表',
        description: '分页查询任务，默认按创建时间倒序；filter / sort 也支持 filter[key]=value 形式的扁平 query 参数',
        query: {
          type: 'object',
          properties: {
            perPage: {
              type: 'number',
              default: 20,
              description: '每页条数'
            },
            currentPage: {
              type: 'number',
              default: 1,
              description: '页码，从 1 开始'
            },
            filter: {
              type: 'object',
              description: '筛选条件',
              properties: {
                id: {
                  type: 'string',
                  description: '任务ID'
                },
                targetId: {
                  type: 'string',
                  description: '目标对象ID'
                },
                targetName: {
                  type: 'string',
                  description: '目标对象名称，对应任务 input.name'
                },
                type: {
                  type: 'string',
                  description: '任务类型'
                },
                status: {
                  type: 'string',
                  description: TASK_STATUS
                },
                runnerType: {
                  type: 'string',
                  description: RUNNER_TYPE
                },
                createdAt: timeRange('createdAt'),
                completedAt: timeRange('completedAt')
              }
            },
            sort: {
              type: 'object',
              description:
                "排序，键为字段名、值为 ASC 升序 / DESC 降序，如 { completedAt: 'DESC' }；可排序字段：id、targetId、targetType、type、status、runnerType、priority、createdAt、updatedAt、startTime、startedAt、completedAt，其他字段报错"
            }
          }
        },
        response: success('任务分页结果', taskList)
      }
    },
    async request => {
      return services.list(request.query);
    }
  );

  fastify.post(
    `${options.prefix}/complete`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        tags: [TAG],
        summary: '手动完成任务',
        description: '人工把任务标记为成功或失败并记录完成人；成功时执行任务类型的完成回调，回调出错则转为失败；只有未结束的任务会被更新；只接受下列声明的字段，其它字段（如 context、userId）会被忽略',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID'
            },
            status: {
              type: 'string',
              enum: ['success', 'failed'],
              description: '完成结果：success 成功，failed 失败'
            },
            error: {
              type: 'string',
              description: '失败原因，status 为 failed 时记录，不传记为「任务失败」'
            },
            msg: {
              type: 'string',
              description: '任务消息，status 为 failed 时写入任务'
            },
            output: {
              type: 'object',
              description: '任务输出数据'
            }
          },
          required: ['id', 'status']
        },
        response: empty('操作成功，返回空对象')
      }
    },
    async request => {
      // 只透传 schema 声明的字段，避免客户端借失败分支覆盖 context.secret 等任意列
      const { id, status, error, msg, output } = request.body;
      await services.complete({ id, status, error, msg, output, userId: request.userInfo?.id });
      return {};
    }
  );

  fastify.post(
    `${options.prefix}/cancel`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        tags: [TAG],
        summary: '取消任务',
        description: '把 pending / running 状态的任务置为 canceled：传 id 取消单个任务；同时传 targetId、targetType、type 时批量取消该目标下该类型的任务（优先于 id）；两种都没传报错',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID'
            },
            targetId: {
              type: 'string',
              description: '目标对象ID（批量取消时使用）'
            },
            targetType: {
              type: 'string',
              description: '目标对象类型（批量取消时使用）'
            },
            type: {
              type: 'string',
              description: '任务类型（批量取消时使用）'
            }
          }
        },
        response: empty('操作成功，返回空对象')
      }
    },
    async request => {
      await services.cancel(request.body);
      return {};
    }
  );

  fastify.post(
    `${options.prefix}/retry`,
    {
      onRequest: options.getAuthenticate('write'),
      schema: {
        tags: [TAG],
        summary: '重试任务',
        description: '把 failed / canceled 状态的任务重置为 pending 并清空完成信息、错误和重试计数，等待重新调度；其他状态报错，批量时遇到错误即中止',
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID（单个重试）'
            },
            taskIds: {
              type: 'array',
              items: {
                type: 'string'
              },
              description: '任务ID列表（批量重试），可与 id 同时传'
            }
          }
        },
        response: empty('操作成功，返回空对象')
      }
    },
    async request => {
      await services.retry(request.body);
      return {};
    }
  );

  fastify.post(
    `${options.prefix}/next`,
    {
      schema: {
        tags: [CALLBACK_TAG],
        summary: '处理任务next',
        description: `外部服务回传 waiting 状态任务的执行结果：result 为 JSON 字符串，code 为 0 时任务成功（data 交给任务完成回调），否则任务失败；${SIGNATURE_HINT}。该接口不鉴权`,
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID'
            },
            signature: {
              type: 'string',
              description: '签名：以任务 context.secret 为密钥，对 `${id}|${result}` 做 HMAC-SHA256 的十六进制摘要'
            },
            result: {
              type: 'string',
              description: '结果 JSON 字符串，形如 {"code":0,"data":{}}；解析失败任务置为失败'
            }
          }
        },
        response: { 200: { description: '处理完成，响应体为空' } }
      }
    },
    async request => {
      return await services.processNext(request.body);
    }
  );

  fastify.post(
    `${options.prefix}/log`,
    {
      schema: {
        tags: [CALLBACK_TAG],
        summary: '记录任务日志',
        description: `外部服务为任务追加一条日志，保存在任务 options.logs（新的在前，最多 100 条）；${SIGNATURE_HINT}。该接口不鉴权`,
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID，与 query.taskId 二选一，优先使用 id'
            },
            data: {
              type: 'object',
              description: '日志数据'
            },
            message: {
              type: 'string',
              description: '日志内容'
            },
            signature: {
              type: 'string',
              description: 'HMAC-SHA256签名'
            }
          }
        },
        query: {
          type: 'object',
          properties: {
            taskId: {
              type: 'string',
              description: '任务ID，body 未传 id 时使用'
            }
          }
        },
        response: success('更新后的任务记录', task)
      }
    },
    async request => {
      return await services.logWithSignature(Object.assign({}, request.body, { taskId: request.query.taskId }));
    }
  );

  fastify.post(
    `${options.prefix}/callback`,
    {
      schema: {
        tags: [CALLBACK_TAG],
        summary: '任务回调',
        description: `外部服务回传任务最终结果：先把回调内容记入任务日志，code 为 0 时任务成功（data 作为输出），否则任务失败（message 作为错误）；${SIGNATURE_HINT}。该接口不鉴权`,
        body: {
          type: 'object',
          properties: {
            id: {
              type: 'string',
              description: '任务ID，与 query.taskId 二选一，优先使用 id'
            },
            code: {
              type: 'number',
              description: '结果码，0 表示成功，其他表示失败'
            },
            data: {
              type: 'object',
              description: '结果数据，作为任务输出'
            },
            message: {
              type: 'string',
              description: '结果说明，失败时作为错误信息'
            },
            signature: {
              type: 'string',
              description: 'HMAC-SHA256签名'
            }
          }
        },
        query: {
          type: 'object',
          properties: {
            taskId: {
              type: 'string',
              description: '任务ID，body 未传 id 时使用'
            }
          }
        },
        response: { 200: { description: '处理完成，响应体为空' } }
      }
    },
    async request => {
      return await services.callbackWithSignature(Object.assign({}, request.body, { taskId: request.query.taskId }));
    }
  );
});

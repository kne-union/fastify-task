# fastify-task

### 描述

一个 Fastify 插件，用于任务编排、追踪任务执行状态和结果。支持系统自动执行和手动执行两种模式，提供完整的任务生命周期管理

### 安装

```shell
npm i --save @kne/fastify-task
```

### 概述

`@kne/fastify-task` 是一个 Fastify 插件，用于任务编排、执行状态追踪、结果回调、日志记录和统计分析。插件支持 **system 自动执行
** 与 **manual 手动完成** 两种模式，适合处理导出、同步、通知、外部异步回调等后台任务场景。

### 核心架构与流程

```
创建任务 (services.create)
  ↓
pending（待执行）
  ├→ manual：services.complete → success / failed
  └→ system：cron 触发 runner
        ↓
      checkTimeout（检测 running / waiting 超时任务）
        ↓
      claimPendingTasks（按优先级与 startTime 原子认领）
        ↓
      running（执行中）
        ├→ executor 正常完成 → handler → next task → success
        ├→ executor 调用 next() → waiting
        ├→ executor 异常 → retry / failed
        └→ waiting → processNext / callbackWithSignature → handler → next task → success / failed
```

| 节点                      | 说明                                                  |
|-------------------------|-----------------------------------------------------|
| `checkTimeout`          | 每次调度前检测 `running` / `waiting` 中已超过任务级 `timeout` 的任务 |
| `claimPendingTasks`     | 使用带状态条件的更新将 `pending` 原子认领为 `running`，避免多实例重复执行     |
| `processSystemTask`     | 执行任务脚本，并根据结果、异常、重试次数和回调状态更新任务                       |
| `collectTaskStatistics` | 任务进入终态后采集数量和耗时指标                                    |
| `taskRegistry`          | 维护主项目与插件通过 `append` 注册的任务声明、脚本目录和生命周期钩子             |

> **关键设计**：cron 每次触发 `runner` 时先检测超时任务，再按 **优先级降序 + startTime 升序** 认领待执行系统任务。认领通过条件更新完成，只有仍处于
`pending` 的任务会被当前实例执行。

### 核心概念详解

#### 任务状态

| 状态         | 说明     | 可流转到                                       |
|------------|--------|--------------------------------------------|
| `pending`  | 待执行    | `running`、`canceled`                       |
| `running`  | 执行中    | `success`、`failed`、`waiting`、`pending`（重试） |
| `waiting`  | 等待外部回调 | `success`、`failed`                         |
| `success`  | 执行成功   | 终态                                         |
| `failed`   | 执行失败   | `pending`（重试）                              |
| `canceled` | 已取消    | `pending`（重试）                              |

#### 执行模式

| 模式   | runnerType | 触发方式                 | 说明                   |
|------|------------|----------------------|----------------------|
| 系统自动 | `system`   | cron 定时调度 `runner`   | 按优先级和 startTime 自动执行 |
| 手动执行 | `manual`   | 用户通过 `complete` 接口完成 | 任务创建后等待人工操作          |

#### 调度与恢复

| 机制   | 触发时机                     | 说明                                                                |
|------|--------------------------|-------------------------------------------------------------------|
| 并发上限 | 每次 `runner()` 执行         | 当前 `running` 系统任务数达到 `limit` 时不再认领新任务                             |
| 原子认领 | 查询到候选 `pending` 任务后      | 通过 `id + runnerType + status + startTime` 条件更新为 `running`，认领失败则跳过 |
| 启动恢复 | Fastify `onReady`        | 默认只将超过 `recoverRunningTaskAfter` 的陈旧 `running` 任务恢复为 `pending`    |
| 手动重置 | 调用 `services.resetAll()` | 不传 `staleOnly` 时会将所有 `running` 任务重置为 `pending`                    |

### 主要特性

| 特性      | 说明                                        |
|---------|-------------------------------------------|
| 任务创建与管理 | 支持创建、取消、重试、完成任务                           |
| 多种执行模式  | system 自动执行 / manual 手动执行                 |
| 定时任务调度  | 基于 cron 表达式定时执行系统任务                       |
| 任务状态追踪  | 实时追踪进度、轮询结果                               |
| 异步回调支持  | 支持 `next` → `processNext`/`callback` 回调链路 |
| 日志记录    | 任务执行日志，最多保留 100 条                         |
| 优先级与依赖  | 优先级排序 + 父子任务链式执行                          |
| 自动重试    | 指数退避重试策略                                  |
| 超时控制    | 全局执行超时和任务级超时均使用毫秒，超时自动标记 failed           |
| 多实例调度   | 待执行任务通过原子认领进入 running，降低重复执行风险            |
| 插件任务注册  | 插件可通过 `append` 注册自己的任务目录、脚本名、处理函数和错误处理     |
| 链式任务    | 支持通过 `next` 声明后续任务，按当前输出创建下一任务              |
| 统计面板    | 任务统计数据查询 + SSE 实时推送                       |

### 使用方法

#### 插件注册与配置

```js
// 基础注册
const fastify = require('fastify')();
fastify.register(require('@kne/fastify-task'), {
  // 可选配置项，参见 api.md 配置项章节
});
```

```js
// 带任务类型处理函数的注册
fastify.register(require('@kne/fastify-task'), {
  task: {
    'export-excel': async ({ task, result, context }) => {
      console.log('导出任务完成:', result);
    },
    'send-email': {
      handler: async ({ task, result }) => {
        // 发送邮件完成后的处理
      },
      errorHandler: async ({ task, error }) => {
        // 可选：当前任务类型自己的错误处理
      }
    }
  }
});
```

#### 任务声明与命名空间

任务类型可以由主项目注册时的 `task` 配置声明，也可以由其他插件通过 `append` 动态声明。声明对象支持 `handler`、`errorHandler` 和
`next`，其中 `handler` 可省略；只要任务类型已经声明并存在对应执行脚本，就可以创建和执行任务。

| 声明来源       | 默认命名空间       | 任务名解析规则                                      |
|------------|--------------|-----------------------------------------------|
| 主项目 `task` | `options.name` | 可直接使用本地任务名，也可使用 `{options.name}.任务名`          |
| 插件 `append` | `append.name` | 本地任务名只在当前插件内查找，跨插件使用 `{append.name}.任务名` |

> **关键设计**：插件任务不能访问主项目中的任务。插件内的裸任务名只解析到当前插件命名空间，跨插件访问必须使用 `包名.任务名`。

#### 任务脚本开发

在 `dirs` 配置的目录下创建任务脚本，目录结构：

```
libs/tasks/
└── <任务类型>/
    └── <scriptName>.js      # 默认 scriptName 为 'index'
```

**脚本模板：**

```js
module.exports = async (fastify, options, { task, updateProgress, polling, next, log }) => {
  const { input } = task;

  // 更新进度 (0-100)
  await updateProgress(50);

  // 轮询外部服务
  const result = await polling(async () => {
    const res = await fetch('external-api');
    return {
      result: 'success | failed | pending',
      data: res.data,
      message: '消息',
      progress: 80
    };
  }, {
    maxPollTimes: 20,
    pollInterval: 10000
  });

  // 等待外部回调（任务状态变为 waiting）
  await next({
    secret: '签名密钥',
    // 其他上下文数据，回调时可在 context 中获取
  });

  return { /* 返回结果 */ };
};
```

#### executor 辅助方法

| 方法签名                         | 说明                               |
|------------------------------|----------------------------------|
| `updateProgress(progress)`   | 更新任务进度 (0-100)                   |
| `polling(callback, options)` | 轮询外部服务直到完成                       |
| `next(context)`              | 设置任务为 waiting 状态，返回 `false` 暂停执行 |
| `log({ data, message })`     | 记录任务执行日志                         |

**polling options 参数：**

| 参数名            | 类型     | 默认值     | 说明       |
|----------------|--------|---------|----------|
| `maxPollTimes` | number | `20`    | 最大轮询次数   |
| `pollInterval` | number | `10000` | 轮询间隔（毫秒） |

**polling callback 返回格式：**

| 字段         | 类型     | 说明                                     |
|------------|--------|----------------------------------------|
| `result`   | string | `'success'` / `'failed'` / `'pending'` |
| `data`     | object | 成功时返回的数据                               |
| `message`  | string | 消息                                     |
| `progress` | number | 当前进度                                   |

#### 任务类型处理函数

任务成功后如果配置了 `handler`，会自动调用对应处理函数；未配置时直接进入成功收尾。处理函数接收参数：

| 参数名       | 类型              | 说明                   |
|-----------|-----------------|----------------------|
| `task`    | Task            | 任务实例                 |
| `result`  | object / string | 任务输出结果               |
| `context` | object          | 任务上下文（`next` 时设置的数据） |

#### 错误处理函数

任务最终失败时优先调用任务声明中的 `errorHandler`；未配置时调用全局 `errorHandler`。如果任务级 `errorHandler` 抛出异常，仍会继续交给全局
`errorHandler` 处理。

| 处理器                    | 触发条件          | 说明                         |
|------------------------|---------------|----------------------------|
| `tasks[type].errorHandler` | 当前任务最终失败      | 当前任务类型自己的错误处理，优先级最高       |
| `options.errorHandler` | 未配置任务级处理或任务级处理抛错 | 全局兜底错误处理，不改变任务最终失败状态      |

#### 运行时动态添加

```js
// 插件包通过 append 方法运行时添加任务目录和类型
const result = await fastify.task.services.append({
  name: 'trtcConference',
  dir: '/path/to/tasks',
  scriptName: 'index',
  tasks: {
    'record-video': {
      handler: async ({ task, result }) => {
        // 可选：任务成功后的处理逻辑
      },
      errorHandler: async ({ task, error }) => {
        // 可选：任务失败后的处理逻辑
      },
      next: ['transcode-video', 'analyze-video']
    }
  }
});
// result.dirs  → 实际添加的目录列表
// result.tasks → 实际添加的类型列表
```

`append.scriptName` 只影响本次 append 注册的任务；未配置时固定使用 `index.js`，不会使用主项目注册 task 时的 `options.scriptName`。

#### 链式任务 next

`tasks[type].next` 用于声明当前任务成功后的后续任务，只配置任务名称：

| next 类型 | 下一个任务选择方式                         |
|---------|------------------------------------|
| string  | 固定创建该任务名对应的下一任务                  |
| array   | 从当前任务 `output.next` 读取下一任务名，并要求在数组中 |

创建下一任务时，`input` 使用当前任务 `output`，`input.name` 沿用当前任务的 `input.name`，`targetType` 固定为 `task`，`targetId` 和
`parentTaskId` 为当前任务 id，`context` 从当前任务深拷贝。

**业务示例：会议录像处理**

会议结束后先执行 `record-video` 获取录像文件。脚本根据录像结果决定下一步：如果需要转码则进入 `transcode-video`，如果录像已经可直接使用则进入
`save-video`。

```js
await fastify.task.services.append({
  name: 'trtcConference',
  dir: path.resolve(__dirname, './libs/tasks'),
  tasks: {
    'record-video': {
      handler: async ({ task, result }) => {
        await fastify.trtcConference.services.saveRecordMeta(result);
      },
      next: ['transcode-video', 'save-video']
    },
    'transcode-video': {
      next: 'save-video'
    },
    'save-video': {
      handler: async ({ result }) => {
        await fastify.trtcConference.services.saveRecordVideo(result);
      }
    }
  }
});
```

`record-video/index.js` 可以通过 `output.next` 指定下一步：

```js
module.exports = async (fastify, options, { task }) => {
  const record = await fastify.trtc.services.checkRecord(task.input);
  return {
    name: task.input.name,
    fileId: record.fileId,
    fileUrl: record.fileUrl,
    next: record.needTranscode ? 'transcode-video' : 'save-video'
  };
};
```

如果 `record.needTranscode` 为 `true`，系统会创建 `transcode-video` 任务；否则创建 `save-video` 任务。下一任务的 `input` 就是上面返回的
`output`，并自动带上当前任务的 `context`。

**业务示例：多级审批**

合同审批需要依次经过部门主管、财务和法务。每一级审批任务只负责处理当前节点，并把审批轨迹写入 `context.approvals`。下一任务创建时会复制当前
`context`，因此后续节点可以看到前面所有审批记录。

```js
await fastify.task.services.append({
  name: 'contractApproval',
  dir: path.resolve(__dirname, './libs/tasks'),
  tasks: {
    'manager-approval': {
      next: 'finance-approval'
    },
    'finance-approval': {
      next: ['legal-approval', 'archive-contract']
    },
    'legal-approval': {
      next: 'archive-contract'
    },
    'archive-contract': {
      handler: async ({ task, context }) => {
        await fastify.contract.services.archive({
          contractId: task.input.contractId,
          approvals: context.approvals
        });
      }
    }
  }
});
```

创建第一个审批任务时写入初始上下文：

```js
await fastify.task.services.create({
  type: 'contractApproval.manager-approval',
  targetType: 'contract',
  targetId: contract.id,
  runnerType: 'system',
  input: {
    name: contract.name,
    contractId: contract.id
  },
  context: {
    requestId: contract.requestId,
    approvals: []
  }
});
```

`manager-approval/index.js` 更新当前任务的 `context`：

```js
module.exports = async (fastify, options, { task }) => {
  const approvals = [...(task.context.approvals || []), {
    node: 'manager',
    approver: 'u-manager-1',
    result: 'approved',
    time: new Date().toISOString()
  }];

  await task.update({
    context: {
      ...task.context,
      approvals
    }
  });

  return {
    name: task.input.name,
    contractId: task.input.contractId
  };
};
```

`finance-approval/index.js` 可以根据金额动态决定是否进入法务审批：

```js
module.exports = async (fastify, options, { task }) => {
  const approvals = [...(task.context.approvals || []), {
    node: 'finance',
    approver: 'u-finance-1',
    result: 'approved',
    time: new Date().toISOString()
  }];

  await task.update({
    context: {
      ...task.context,
      approvals
    }
  });

  return {
    name: task.input.name,
    contractId: task.input.contractId,
    next: task.input.amount > 100000 ? 'legal-approval' : 'archive-contract'
  };
};
```

在这个流程中，`finance-approval` 会拿到主管审批后复制过来的 `context.approvals`；`legal-approval` 或 `archive-contract`
也会继续拿到财务审批后更新过的 `context`。每次创建下一任务都会深拷贝当前任务完成时的 `context`，不会和上一任务共享同一个对象引用。

#### 签名验证

当任务通过 `next({ secret: '密钥' })` 设置了密钥时，外部回调需提供 HMAC-SHA256 签名。

**签名生成方法：**

```js
const crypto = require('node:crypto');

function generateSignature({ secret, id, data }) {
  const dataStr = typeof data === 'string' ? data : JSON.stringify(data);
  const dataToSign = `${id}|${dataStr}`;
  const hmac = crypto.createHmac('sha256', secret);
  hmac.update(dataToSign);
  return hmac.digest('hex');
}
```

**各接口签名数据格式：**

| 接口                      | data 格式                      |
|-------------------------|------------------------------|
| `processNext`           | `result` 字符串（JSON 格式结果）      |
| `logWithSignature`      | `{ data, message }` 对象       |
| `callbackWithSignature` | `{ code, data, message }` 对象 |

```js
// processNext 签名
const result = JSON.stringify({ code: 0, data: { output: 'done' } });
const signature = generateSignature({ secret: 'your-secret', id: 'task-1', data: result });

// callbackWithSignature 签名
const signature = generateSignature({
  secret: 'your-secret', id: 'task-1',
  data: { code: 0, data: { result: 'done' }, message: '成功' }
});
```

> **关键设计**：未设置 `context.secret` 时签名验证自动跳过，不阻塞无密钥任务的回调。

#### 插件依赖

| 依赖                        | 说明      |
|---------------------------|---------|
| `fastify-cron`            | 定时任务支持  |
| `@kne/fastify-namespace`  | 命名空间模块化 |
| `@kne/fastify-statistics` | 统计数据采集  |
| `fastify-sequelize`       | 数据库模型支持 |


### 示例

### API

### 配置项

| 属性名                          | 类型            | 必填 | 默认值              | 说明                                                                             |
|------------------------------|---------------|----|------------------|--------------------------------------------------------------------------------|
| `dbTableNamePrefix`          | string        | 否  | `'t_'`           | 数据库表名前缀                                                                        |
| `prefix`                     | string        | 否  | `'/api/task'`    | API 路由前缀                                                                       |
| `name`                       | string        | 否  | `'task'`         | 插件命名空间名称                                                                       |
| `limit`                      | number        | 否  | `10`             | 系统任务并发执行上限                                                                     |
| `dir`                        | string        | 否  | `'libs/tasks'`   | 任务脚本目录（向后兼容，优先级高于 dirs）                                                        |
| `dirs`                       | array         | 否  | `null`           | 任务脚本目录列表，运行时可通过 `append` 动态添加                                                  |
| `cronTime`                   | string        | 否  | `'*/10 * * * *'` | 定时任务 Cron 表达式                                                                  |
| `scriptName`                 | string        | 否  | `'index'`        | 默认任务脚本名称                                                                       |
| `maxPollTimes`               | number        | 否  | `20`             | 最大轮询次数                                                                         |
| `pollInterval`               | number        | 否  | `10000`          | 轮询间隔（毫秒）                                                                       |
| `taskTimeout`                | number        | 否  | `1800000`        | 任务执行超时时间（毫秒），0 表示不超时                                                           |
| `recoverRunningTaskAfter`    | number / null | 否  | `null`           | 启动恢复 running 任务的陈旧阈值（毫秒）；未传时使用 `taskTimeout`，若 `taskTimeout` 为 0 则使用 `1800000` |
| `recoverRunningTasksOnStart` | boolean       | 否  | `true`           | 启动时是否恢复陈旧 running 任务                                                           |
| `retryBaseDelay`             | number        | 否  | `5000`           | 重试基础延迟（毫秒），实际延迟 = `retryBaseDelay * 2^(retryCount-1)`                          |
| `errorHandler`               | function      | 否  | -                | 全局错误处理函数，任务级 `errorHandler` 未配置或抛错时调用                                           |
| `getUserModel`               | function      | 否  | -                | 获取用户 Model 的函数                                                                 |
| `getAuthenticate`            | function      | 否  | -                | 获取认证中间件的函数                                                                     |
| `task`                       | object        | 否  | `{}`             | 主项目任务声明，支持 `{ [type]: handler }` 或 `{ [type]: { handler, errorHandler, next } }` |

> **关键设计**：`dirs` 初始化逻辑 — 优先使用用户传入的 `dirs`，否则以 `dir` 为默认值；若 `dirs` 中不包含 `dir`，则将 `dir`
> 插入 `dirs` 首位，保证向后兼容。

> **关键设计**：`recoverRunningTasksOnStart` 只恢复陈旧 `running` 任务，避免多实例部署时某个实例重启误重置其他实例正在执行的任务。

### HTTP 接口

#### POST `{prefix}/create`

创建任务，需 `write` 权限。

| 参数名          | 类型     | 必填 | 默认值       | 说明                        |
|--------------|--------|----|-----------|---------------------------|
| type         | string | 是  | -         | 任务类型，必须已在主项目或 `append` 中声明 |
| targetId     | string | 是  | -         | 目标对象ID                    |
| targetType   | string | 是  | -         | 目标对象类型                    |
| targetName   | string | 否  | -         | 目标对象名称                    |
| input        | object | 否  | -         | 输入数据                      |
| runnerType   | string | 否  | -         | 执行者类型：`manual` / `system` |
| delay        | number | 否  | `0`       | 延迟执行秒数                    |
| scriptName   | string | 否  | -         | 任务脚本名称                    |
| priority     | number | 否  | `0`       | 优先级，数值越大越优先               |
| parentTaskId | string | 否  | -         | 父任务ID，用于任务依赖              |
| maxRetries   | number | 否  | `0`       | 最大自动重试次数                  |
| timeout      | number | 否  | `3600000` | 任务超时时间（毫秒），0 表示不超时        |
| context      | object | 否  | `{}`      | 初始上下文，用于回调签名、日志或链式任务传递     |

返回值：

```json
{
  "id": "task-1"
}
```

#### GET `{prefix}/list`

获取任务列表，需 `read` 权限。

| 参数名                          | 类型     | 必填 | 默认值  | 说明                                                  |
|------------------------------|--------|----|------|-----------------------------------------------------|
| perPage                      | number | 否  | `20` | 每页数量                                                |
| currentPage                  | number | 否  | `1`  | 当前页码                                                |
| filter.id                    | string | 否  | -    | 任务ID                                                |
| filter.targetId              | string | 否  | -    | 目标对象ID                                              |
| filter.targetName            | string | 否  | -    | 目标名称（模糊匹配 `input.name`）                             |
| filter.type                  | string | 否  | -    | 任务类型                                                |
| filter.status                | string | 否  | -    | 任务状态                                                |
| filter.runnerType            | string | 否  | -    | 执行者类型                                               |
| filter.createdAt.startTime   | string | 否  | -    | 创建时间起始                                              |
| filter.createdAt.endTime     | string | 否  | -    | 创建时间结束                                              |
| filter.completedAt.startTime | string | 否  | -    | 完成时间起始                                              |
| filter.completedAt.endTime   | string | 否  | -    | 完成时间结束                                              |
| sort                         | object | 否  | -    | 排序规则（支持 `completedAt`、`updatedAt` 等字段，`ASC`/`DESC`） |

返回值：

```json
{
  "pageData": [],
  "totalCount": 0
}
```

#### POST `{prefix}/complete`

手动完成任务，需 `write` 权限。

| 参数名    | 类型     | 必填 | 说明                        |
|--------|--------|----|---------------------------|
| id     | string | 是  | 任务ID                      |
| status | string | 是  | 完成状态：`success` / `failed` |
| error  | string | 否  | 错误信息                      |
| msg    | string | 否  | 消息                        |
| output | object | 否  | 输出数据                      |

返回空对象：

```json
{}
```

#### POST `{prefix}/cancel`

取消任务，需 `write` 权限。支持单个取消（`id`）或批量取消（`targetId` + `targetType` + `type`）。

| 参数名        | 类型     | 必填 | 说明                                     |
|------------|--------|----|----------------------------------------|
| id         | string | 否  | 任务ID（单个取消）                             |
| targetId   | string | 否  | 目标对象ID（批量取消，需配合 `targetType` 和 `type`） |
| targetType | string | 否  | 目标对象类型（批量取消）                           |
| type       | string | 否  | 任务类型（批量取消）                             |

返回空对象：

```json
{}
```

#### POST `{prefix}/retry`

重试任务，需 `write` 权限。仅允许重试 `failed` 或 `canceled` 状态的任务。

| 参数名     | 类型       | 必填 | 说明     |
|---------|----------|----|--------|
| id      | string   | 否  | 单个任务ID |
| taskIds | array | 否  | 任务ID数组 |

返回空对象：

```json
{}
```

#### POST `{prefix}/next`

处理等待回调的任务，无需认证。当任务设置了 `context.secret` 时需提供签名。

| 参数名       | 类型     | 必填 | 说明                                       |
|-----------|--------|----|------------------------------------------|
| id        | string | 是  | 任务ID                                     |
| signature | string | 否  | HMAC-SHA256 签名（当 `context.secret` 存在时必填） |
| result    | string | 是  | JSON 格式结果字符串                             |

成功处理时返回空对象：

```json
{}
```

#### POST `{prefix}/log`

记录任务日志，无需认证。当任务设置了 `context.secret` 时需提供签名。

| 参数名       | 类型     | 必填 | 说明                                       |
|-----------|--------|----|------------------------------------------|
| id        | string | 否  | 任务ID（body）                               |
| taskId    | string | 否  | 任务ID（query，替代 body 中的 `id`）              |
| data      | object | 否  | 日志数据                                     |
| message   | string | 否  | 日志消息                                     |
| signature | string | 否  | HMAC-SHA256 签名（当 `context.secret` 存在时必填） |

返回更新后的 Task 实例：

```json
{
  "id": "task-1",
  "options": {
    "logs": []
  }
}
```

#### POST `{prefix}/callback`

任务回调，无需认证。当任务设置了 `context.secret` 时需提供签名。

| 参数名       | 类型     | 必填 | 说明                                       |
|-----------|--------|----|------------------------------------------|
| id        | string | 否  | 任务ID（body）                               |
| taskId    | string | 否  | 任务ID（query，替代 body 中的 `id`）              |
| code      | number | 是  | 状态码，0 为成功                                |
| data      | object | 否  | 回调数据                                     |
| message   | string | 否  | 回调消息                                     |
| signature | string | 否  | HMAC-SHA256 签名（当 `context.secret` 存在时必填） |

成功处理时返回空对象：

```json
{}
```

#### GET `{prefix}/statistics`

获取任务统计概览，需 `statistics` 权限。

| 参数名        | 类型     | 必填 | 默认值    | 说明                             |
|------------|--------|----|--------|--------------------------------|
| range      | string | 否  | `'7d'` | 时间范围：`7d` / `1m` / `3m` / `1y` |
| timezone   | string | 否  | 服务器时区  | 时区，如 `Asia/Shanghai`           |
| type       | string | 否  | -      | 按任务类型筛选                        |
| runnerType | string | 否  | -      | 按执行方式筛选 `manual` / `system`    |

返回值：

```json
{
  "range": "7d",
  "rangeLabel": "最近7天",
  "totalTasks": 0,
  "byStatus": {},
  "byType": {},
  "byRunnerType": {},
  "recentTrend": [],
  "recentTrendByStatus": [],
  "recentTrendByType": [],
  "durationTrend": [],
  "hourlyTrend": [],
  "hourlyCompletionTrend": []
}
```

#### GET `{prefix}/statistics/sse`

SSE 实时推送任务统计数据，需 `statistics` 权限。

| 参数名        | 类型     | 必填 | 默认值    | 说明               |
|------------|--------|----|--------|------------------|
| range      | string | 否  | `'7d'` | 时间范围             |
| timezone   | string | 否  | 服务器时区  | 时区               |
| type       | string | 否  | -      | 按任务类型筛选          |
| runnerType | string | 否  | -      | 按执行方式筛选          |
| interval   | number | 否  | `5`    | 推送间隔时间（秒），最小 1 秒 |

### 程序化 API

通过 `fastify.task.services` 访问。

#### 任务管理

| 方法签名                                                                                                                                                                       | 说明                                   |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------|
| `services.create({ userId, input, type, targetId, targetType, targetName, runnerType, delay, scriptName, priority, parentTaskId, maxRetries, timeout, context, options })` | 创建任务，返回 Task 实例                      |
| `services.detail({ id })`                                                                                                                                                  | 获取任务详情，返回 Task 实例                    |
| `services.list({ filter, perPage, currentPage, sort })`                                                                                                                    | 获取任务列表，返回 `{ pageData, totalCount }` |
| `services.complete({ id, userId, status, output, error })`                                                                                                                 | 手动完成任务                               |
| `services.cancel({ id, targetId, targetType, type })`                                                                                                                      | 取消任务，支持单个或批量                         |
| `services.retry({ id, taskIds })`                                                                                                                                          | 重试任务，仅允许 `failed`/`canceled` 状态      |
| `services.waitingComplete({ id, pollInterval, maxPollTimes })`                                                                                                             | 等待任务完成（轮询），返回任务输出数据                  |

**services.create 参数：**

| 参数名          | 类型     | 必填 | 默认值       | 说明       |
|--------------|--------|----|-----------|----------|
| userId       | string | 否  | -         | 用户ID     |
| input        | object | 否  | -         | 输入数据     |
| type         | string | 是  | -         | 任务类型     |
| targetId     | string | 是  | -         | 目标对象ID   |
| targetType   | string | 是  | -         | 目标对象类型   |
| targetName   | string | 否  | -         | 目标对象名称   |
| runnerType   | string | 否  | -         | 执行者类型    |
| delay        | number | 否  | `0`       | 延迟执行秒数   |
| scriptName   | string | 否  | -         | 脚本名称     |
| priority     | number | 否  | `0`       | 优先级      |
| parentTaskId | string | 否  | -         | 父任务ID    |
| maxRetries   | number | 否  | `0`       | 最大重试次数   |
| timeout      | number | 否  | `3600000` | 超时时间（毫秒） |
| context      | object | 否  | `{}`      | 初始上下文    |
| options      | object | 否  | -         | 扩展选项     |

**services.waitingComplete 参数：**

| 参数名          | 类型     | 必填 | 默认值    | 说明       |
|--------------|--------|----|--------|----------|
| id           | string | 是  | -      | 任务ID     |
| pollInterval | number | 否  | `1000` | 轮询间隔（毫秒） |
| maxPollTimes | number | 否  | `20`   | 最大轮询次数   |

#### 回调与日志

| 方法签名                                                                     | 说明                       |
|--------------------------------------------------------------------------|--------------------------|
| `services.processNext({ id, signature, result })`                        | 处理等待回调的任务，需签名验证          |
| `services.callback({ id, code, data, message })`                         | 任务回调（内部调用，无需签名）          |
| `services.callbackWithSignature({ id, code, data, message, signature })` | 任务回调（外部调用，需签名验证）         |
| `services.log({ id, taskId, data, message })`                            | 记录日志（内部调用，无需签名），最多 100 条 |
| `services.logWithSignature({ id, taskId, data, message, signature })`    | 记录日志（外部调用，需签名验证）         |

#### 系统调度

| 方法签名                                       | 说明                                                   |
|--------------------------------------------|------------------------------------------------------|
| `services.runner()`                        | 执行系统任务，由 cron 定时调用                                   |
| `services.claimPendingTasks(limit)`        | 按优先级和开始时间认领待执行系统任务，返回已认领任务列表                         |
| `services.processSystemTask(task, opts)`   | 执行系统任务；`opts.claimed` 为 `true` 时跳过重复置为 running       |
| `services.resetAll({ staleOnly, before })` | 重置 running 任务为 pending；`staleOnly` 为 `true` 时只恢复陈旧任务 |
| `services.append({ name, dir, dirs, scriptName, tasks, override })` | 运行时动态添加任务声明，返回 `{ dirs, tasks, skippedTasks }`         |

**services.append 参数：**

| 参数名        | 类型      | 必填 | 默认值            | 说明                                      |
|-------------|---------|----|----------------|-----------------------------------------|
| name        | string  | 否  | `options.name` | 本次 append 注册的任务命名空间，插件包通常传自身 `options.name` |
| dir         | string  | 否  | -              | 任务脚本目录，等价于单项 `dirs`                    |
| dirs        | array   | 否  | -              | 任务脚本目录列表                                |
| scriptName  | string  | 否  | `'index'`      | 本次 append 注册任务的默认脚本名                    |
| tasks       | object  | 否  | -              | 任务声明映射，支持函数简写或对象配置                    |
| override    | boolean | 否  | `false`        | 是否覆盖已存在的同名任务声明                         |

**tasks[type] 对象配置：**

| 属性名          | 类型             | 必填 | 默认值 | 说明                                      |
|---------------|----------------|----|-----|-----------------------------------------|
| handler       | function       | 否  | -   | 任务成功后的处理函数                              |
| errorHandler  | function       | 否  | -   | 当前任务类型自己的错误处理函数                         |
| next          | string / array | 否  | -   | 成功后创建的下一任务；数组模式从 `output.next` 中选择下一任务 |

> **关键设计**：`append.scriptName` 只影响本次 append 注册的任务。未传时固定使用 `index.js`，不会使用主项目注册 task 时的
> `options.scriptName`。

#### 统计查询

| 方法签名                                                                             | 说明           |
|----------------------------------------------------------------------------------|--------------|
| `services.queryStatistics({ range, timezone, type, runnerType })`                | 查询任务统计数据     |
| `services.sseStatistics({ range, timezone, type, runnerType, interval }, reply)` | SSE 实时推送统计数据 |

**services.queryStatistics 参数：**

| 参数名        | 类型     | 必填 | 默认值    | 说明                             |
|------------|--------|----|--------|--------------------------------|
| range      | string | 否  | `'7d'` | 时间范围：`7d` / `1m` / `3m` / `1y` |
| timezone   | string | 否  | 服务器时区  | 时区                             |
| type       | string | 否  | -      | 按任务类型筛选                        |
| runnerType | string | 否  | -      | 按执行方式筛选                        |

**services.sseStatistics 参数：**

| 参数名        | 类型     | 必填 | 默认值    | 说明                      |
|------------|--------|----|--------|-------------------------|
| range      | string | 否  | `'7d'` | 时间范围                    |
| timezone   | string | 否  | 服务器时区  | 时区                      |
| type       | string | 否  | -      | 按任务类型筛选                 |
| runnerType | string | 否  | -      | 按执行方式筛选                 |
| interval   | number | 否  | `5`    | 推送间隔（秒）                 |
| reply      | object | 是  | -      | Fastify reply 对象（第二个参数） |

### 数据模型

#### Task

| 属性名             | 类型              | 说明                                                                                      |
|-----------------|-----------------|-----------------------------------------------------------------------------------------|
| type            | string          | 任务类型                                                                                    |
| scriptName      | string          | 任务脚本名称                                                                                  |
| targetId        | string          | 任务目标对象ID                                                                                |
| targetType      | string          | 任务目标对象类型                                                                                |
| targetName      | string          | 任务目标对象名称                                                                                |
| runnerType      | string          | 执行者类型：`manual` / `system`，默认 `manual`                                                   |
| priority        | number          | 任务优先级，数值越大越优先，默认 0                                                                      |
| parentTaskId    | string          | 父任务ID，用于任务依赖                                                                            |
| retryCount      | number          | 已重试次数，默认 0                                                                              |
| maxRetries      | number          | 最大重试次数，0 表示不自动重试                                                                        |
| timeout         | number          | 任务超时时间（毫秒），0 表示不超时，默认 3600000                                                           |
| startTime       | Date            | 任务最早执行时间                                                                                |
| startedAt       | Date            | 任务实际开始执行时间                                                                              |
| completedAt     | Date            | 任务完成时间                                                                                  |
| completedUserId | string          | 完成任务的用户ID                                                                               |
| input           | object          | 输入数据                                                                                    |
| output          | object          | 输出数据                                                                                    |
| error           | string / object | 错误信息                                                                                    |
| status          | string          | 任务状态：`pending` / `running` / `waiting` / `success` / `failed` / `canceled`，默认 `pending` |
| context         | object          | 上下文信息                                                                                   |
| pollResults     | array           | 轮询执行结果                                                                                  |
| pollCount       | number          | 轮询次数，默认 0                                                                               |
| progress        | number          | 任务进度 (0-100)，默认 0                                                                       |
| msg             | string          | 任务消息                                                                                    |
| options         | object          | 任务扩展选项（含 `logs` 日志数组）                                                                   |

**关联关系：**

| 关联             | 外键                | 说明                        |
|----------------|-------------------|---------------------------|
| belongsTo User | `userId`          | 创建人，`as: 'createdUser'`   |
| belongsTo User | `completedUserId` | 完成人，`as: 'completedUser'` |
| belongsTo Task | `parentTaskId`    | 父任务，`as: 'parentTask'`    |

**索引：**

| 索引字段                           |
|--------------------------------|
| `created_at`                   |
| `status`                       |
| `type`                         |
| `runner_type`                  |
| `type, status`                 |
| `runner_type, status`          |
| `target_id, target_type, type` |
| `parent_task_id`               |
| `priority`                     |

### 机制说明

#### 任务声明与命名空间

任务类型由 task registry 统一维护。主项目注册 task 插件时可以通过 `task` 直接声明任务，插件包可以通过 `services.append` 声明自己的任务。

| 来源             | 命名空间          | 可访问任务范围                                      |
|----------------|---------------|-----------------------------------------------|
| 主项目 `task`     | `options.name` | 主项目任务、插件任务；插件任务需使用 `{append.name}.任务名`       |
| 插件 `append`    | `append.name`  | 当前插件任务、其他插件任务；不能访问主项目任务                   |
| `create({type})` | 根据任务名解析       | 无点号时优先主项目任务；跨插件或指定命名空间时使用 `命名空间.任务名` |

`append` 注册的任务可以没有 `handler`，只要存在对应执行脚本即可执行。主项目原有的 `{ [type]: handler }` 写法仍然兼容。

> **关键设计**：插件内的裸任务名只在当前插件命名空间内解析；跨插件访问必须使用 `包名.任务名`。插件任务不允许访问主项目任务。

#### 成功与错误处理

任务执行成功后会先调用当前任务声明的 `handler`，再创建 `next` 任务，最后将当前任务更新为 `success` 并采集统计数据。

| 阶段      | 行为                                      |
|---------|-----------------------------------------|
| handler | 如果声明了 `handler`，等待它执行完成；未声明则跳过          |
| next    | 如果声明了 `next`，解析并创建下一任务                   |
| success | 更新 `status/output/progress/completedAt` 并采集统计 |

任务最终失败时会触发错误处理：

| 处理器                    | 触发时机                         |
|------------------------|------------------------------|
| `tasks[type].errorHandler` | 当前任务最终失败时优先调用                 |
| `options.errorHandler` | 未配置任务级错误处理，或任务级错误处理抛错时调用 |

> **关键设计**：自动重试期间不会触发最终错误处理；只有任务最终进入 `failed` 状态时才调用错误处理函数。

#### next 链式任务

`tasks[type].next` 只声明下一任务名称，不声明下一任务的执行参数。

| next 配置 | 解析规则                                    |
|---------|-----------------------------------------|
| string  | 固定使用该任务名作为下一任务                         |
| array   | 从当前任务 `output.next` 读取任务名，并要求它包含在数组中 |

下一任务创建规则：

| 字段           | 取值                                                |
|--------------|---------------------------------------------------|
| `type`       | 解析后的下一任务声明类型                                      |
| `input`      | 当前任务 `output`，并将 `input.name` 设为当前任务 `input.name` |
| `targetType` | 固定为 `task`                                       |
| `targetId`   | 当前任务 id                                           |
| `parentTaskId` | 当前任务 id                                         |
| `context`    | 当前任务 `context` 的深拷贝                              |
| `runnerType` | 当前任务 `runnerType`                                |

如果 `next` 是数组且 `output.next` 缺失，或 `output.next` 不在允许列表中，当前任务会转为 `failed`。

**业务示例：会议录像处理**

会议结束后先执行 `record-video` 获取录像文件。脚本根据录像结果决定下一步：需要转码时进入 `transcode-video`，否则直接进入 `save-video`。

```js
await fastify.task.services.append({
  name: 'trtcConference',
  dir: path.resolve(__dirname, './libs/tasks'),
  tasks: {
    'record-video': {
      handler: async ({ task, result }) => {
        await fastify.trtcConference.services.saveRecordMeta(result);
      },
      next: ['transcode-video', 'save-video']
    },
    'transcode-video': {
      next: 'save-video'
    },
    'save-video': {
      handler: async ({ result }) => {
        await fastify.trtcConference.services.saveRecordVideo(result);
      }
    }
  }
});
```

`record-video/index.js` 返回 `output.next` 指定下一任务：

```js
module.exports = async (fastify, options, { task }) => {
  const record = await fastify.trtc.services.checkRecord(task.input);
  return {
    name: task.input.name,
    fileId: record.fileId,
    fileUrl: record.fileUrl,
    next: record.needTranscode ? 'transcode-video' : 'save-video'
  };
};
```

当 `output.next` 为 `transcode-video` 时，会创建 `trtcConference.transcode-video`；当它为 `save-video` 时，会创建
`trtcConference.save-video`。下一任务的 `input` 使用当前任务完整 `output`，并复制当前任务 `context`。

**业务示例：多级审批**

合同审批需要依次经过部门主管、财务和法务。每一级审批任务只处理当前节点，并把审批轨迹写入 `context.approvals`。下一任务创建时会复制当前
`context`，因此后续节点可以看到前面所有审批记录。

```js
await fastify.task.services.append({
  name: 'contractApproval',
  dir: path.resolve(__dirname, './libs/tasks'),
  tasks: {
    'manager-approval': {
      next: 'finance-approval'
    },
    'finance-approval': {
      next: ['legal-approval', 'archive-contract']
    },
    'legal-approval': {
      next: 'archive-contract'
    },
    'archive-contract': {
      handler: async ({ task, context }) => {
        await fastify.contract.services.archive({
          contractId: task.input.contractId,
          approvals: context.approvals
        });
      }
    }
  }
});
```

创建第一个审批任务时写入初始上下文：

```js
await fastify.task.services.create({
  type: 'contractApproval.manager-approval',
  targetType: 'contract',
  targetId: contract.id,
  runnerType: 'system',
  input: {
    name: contract.name,
    contractId: contract.id,
    amount: contract.amount
  },
  context: {
    requestId: contract.requestId,
    approvals: []
  }
});
```

`manager-approval/index.js` 更新当前任务的 `context`：

```js
module.exports = async (fastify, options, { task }) => {
  const approvals = [...(task.context.approvals || []), {
    node: 'manager',
    approver: 'u-manager-1',
    result: 'approved',
    time: new Date().toISOString()
  }];

  await task.update({
    context: {
      ...task.context,
      approvals
    }
  });

  return {
    name: task.input.name,
    contractId: task.input.contractId,
    amount: task.input.amount
  };
};
```

`finance-approval/index.js` 可以根据金额动态决定是否进入法务审批：

```js
module.exports = async (fastify, options, { task }) => {
  const approvals = [...(task.context.approvals || []), {
    node: 'finance',
    approver: 'u-finance-1',
    result: 'approved',
    time: new Date().toISOString()
  }];

  await task.update({
    context: {
      ...task.context,
      approvals
    }
  });

  return {
    name: task.input.name,
    contractId: task.input.contractId,
    amount: task.input.amount,
    next: task.input.amount > 100000 ? 'legal-approval' : 'archive-contract'
  };
};
```

在这个流程中，`finance-approval` 会拿到主管审批后复制过来的 `context.approvals`；`legal-approval` 或 `archive-contract`
也会继续拿到财务审批后更新过的 `context`。每次创建下一任务都会深拷贝当前任务完成时的 `context`，不会和上一任务共享同一个对象引用。

#### 签名验证

使用 HMAC-SHA256 算法，签名数据格式为 `{id}|{data}`，其中 `data` 为 JSON 字符串或原始字符串。

| 接口                      | 签名 data 内容                             |
|-------------------------|----------------------------------------|
| `processNext`           | `result` 参数值（原始 JSON 字符串）              |
| `logWithSignature`      | `{ data, message }` 对象的 JSON 序列化       |
| `callbackWithSignature` | `{ code, data, message }` 对象的 JSON 序列化 |

> **关键设计**：未设置 `context.secret` 时 `verifySignature` 直接返回 `true`，不阻塞无密钥任务的回调。

#### 重试策略

任务执行失败时，判断是否满足自动重试条件（`retryCount < maxRetries`）：

| 条件                         | 行为                                      |
|----------------------------|-----------------------------------------|
| `retryCount < maxRetries`  | 重置为 `pending`，`startTime` 设为当前时间 + 退避延迟 |
| `retryCount >= maxRetries` | 标记为 `failed`，记录错误信息                     |

退避公式：`delay = retryBaseDelay × 2^(retryCount - 1)`

> **关键设计**：手动调用 `retry` 接口会重置 `retryCount` 为 0，重新开始重试计数。

#### 超时检测

每次 `runner` 执行时先调用 `checkTimeout`，扫描所有 `running` / `waiting` 且 `timeout > 0` 的任务：

```
当前时间 - startedAt > timeout → 标记为 failed
```

> **关键设计**：`taskTimeout`（全局配置）和 `timeout`（任务级别）均使用毫秒作为单位。全局 `taskTimeout` 在 executor 执行时通过
`Promise.race` 控制；任务 `timeout` 字段在 cron 轮询时检测。

#### 任务认领与启动恢复

| 机制   | 表达式 / 条件                                                    | 说明                                                  |
|------|-------------------------------------------------------------|-----------------------------------------------------|
| 认领候选 | `runnerType = system`、`status = pending`、`startTime <= now` | 按 `priority DESC`、`startTime ASC` 查询候选任务            |
| 原子认领 | `id + runnerType + status + startTime` 条件更新                 | 只有更新影响 1 行的任务会被当前实例执行                               |
| 启动恢复 | `resetAll({ staleOnly: true })`                             | 启动时只恢复超过 `recoverRunningTaskAfter` 的陈旧 `running` 任务 |
| 手动重置 | `resetAll()`                                                | 不传 `staleOnly` 时会重置所有 `running` 任务                  |

> **关键设计**：多实例部署时，任务执行前必须先认领；启动恢复默认只处理陈旧任务，避免误中断其他实例正在执行的任务。

#### 父子任务依赖

当父任务执行成功后，自动激活 `parentTaskId` 指向该父任务且状态为 `pending` 的子任务：

| 子任务 runnerType | 激活行为                        |
|----------------|-----------------------------|
| `system`       | 立即调用 `processSystemTask` 执行 |
| `manual`       | 保持 `pending`，等待手动执行         |

> **关键设计**：只有父任务 **成功** 后才触发子任务，失败或取消不会激活子任务。

#### 统计数据采集

任务完成时通过 `@kne/fastify-statistics` 采集数据，channel 格式为 `{type}:{runnerType}:{completedHour}`，采集维度：

| 维度                          | 单位    | 说明                            |
|-----------------------------|-------|-------------------------------|
| total                       | count | 总完成数                          |
| success / failed / canceled | count | 按状态计数                         |
| waitingTime                 | ms    | 等待时长（startedAt - createdAt）   |
| executionTime               | ms    | 执行时长（completedAt - startedAt） |
| totalTime                   | ms    | 总时长（completedAt - createdAt）  |

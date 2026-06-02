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
        ├→ executor 正常完成 → success
        ├→ executor 调用 next() → waiting
        ├→ executor 异常 → retry / failed
        └→ waiting → processNext / callbackWithSignature → success / failed
```

| 节点                      | 说明                                                  |
|-------------------------|-----------------------------------------------------|
| `checkTimeout`          | 每次调度前检测 `running` / `waiting` 中已超过任务级 `timeout` 的任务 |
| `claimPendingTasks`     | 使用带状态条件的更新将 `pending` 原子认领为 `running`，避免多实例重复执行     |
| `processSystemTask`     | 执行任务脚本，并根据结果、异常、重试次数和回调状态更新任务                       |
| `collectTaskStatistics` | 任务进入终态后采集数量和耗时指标                                    |

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
    'send-email': async ({ task, result }) => {
      // 发送邮件完成后的处理
    }
  }
});
```

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

任务完成后自动调用对应 `task[type]` 处理函数，接收参数：

| 参数名       | 类型              | 说明                   |
|-----------|-----------------|----------------------|
| `task`    | Task            | 任务实例                 |
| `result`  | object / string | 任务输出结果               |
| `context` | object          | 任务上下文（`next` 时设置的数据） |

#### 运行时动态添加

```js
// 通过 append 方法运行时添加任务目录和类型
const result = await fastify.task.services.append({
  dirs: ['/path/to/tasks'],
  tasks: {
    'new-type': async ({ task, result }) => { /* 处理逻辑 */
    }
  }
});
// result.dirs  → 实际添加的目录列表
// result.tasks → 实际添加的类型列表
```

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

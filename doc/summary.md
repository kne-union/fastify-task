@kne/fastify-task 是一个 Fastify 插件，用于任务编排、追踪任务执行状态和结果。支持系统自动执行和手动执行两种模式，提供完整的任务生命周期管理。

### 核心架构与流程

```
创建任务 (create)
  ↓
pending（待执行）
  ↓ (cron 调度 runner / 手动 complete)
running（执行中）
  ├→ executor 执行任务脚本
  │   ├→ 正常完成 → success
  │   ├→ 调用 next() → waiting（等待外部回调）
  │   └→ 异常失败 → 判断重试 → pending / failed
  ├→ waiting → processNext / callbackWithSignature → success / failed
  └→ 超时检测 (checkTimeout) → failed
```

> **关键设计**：cron 每次触发 `runner` 时，先执行 `checkTimeout` 检测超时任务，再按 **优先级降序 + startTime 升序** 取待执行系统任务，受 `limit` 并发上限控制。

### 核心概念详解

#### 任务状态

| 状态 | 说明 | 可流转到 |
|------|------|----------|
| `pending` | 待执行 | `running`、`canceled` |
| `running` | 执行中 | `success`、`failed`、`waiting`、`pending`（重试） |
| `waiting` | 等待外部回调 | `success`、`failed` |
| `success` | 执行成功 | 终态 |
| `failed` | 执行失败 | `pending`（重试） |
| `canceled` | 已取消 | `pending`（重试） |

#### 执行模式

| 模式 | runnerType | 触发方式 | 说明 |
|------|-----------|----------|------|
| 系统自动 | `system` | cron 定时调度 `runner` | 按优先级和 startTime 自动执行 |
| 手动执行 | `manual` | 用户通过 `complete` 接口完成 | 任务创建后等待人工操作 |

### 主要特性

| 特性 | 说明 |
|------|------|
| 任务创建与管理 | 支持创建、取消、重试、完成任务 |
| 多种执行模式 | system 自动执行 / manual 手动执行 |
| 定时任务调度 | 基于 cron 表达式定时执行系统任务 |
| 任务状态追踪 | 实时追踪进度、轮询结果 |
| 异步回调支持 | 支持 `next` → `processNext`/`callback` 回调链路 |
| 日志记录 | 任务执行日志，最多保留 100 条 |
| 优先级与依赖 | 优先级排序 + 父子任务链式执行 |
| 自动重试 | 指数退避重试策略 |
| 超时控制 | 任务级别超时设置，超时自动标记 failed |
| 错误统一处理 | 通过 `errorHandler` 配置统一处理任务失败，支持执行错误、超时、回调失败、重试耗尽等场景 |
| 统计面板 | 任务统计数据查询 + SSE 实时推送 |

### 使用方法

#### 插件注册与配置

```javascript
// 基础注册
const fastify = require('fastify')();
fastify.register(require('@kne/fastify-task'), {
  // 可选配置项，参见 api.md 配置项章节
});
```

```javascript
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

```javascript
// 带错误统一处理的注册
fastify.register(require('@kne/fastify-task'), {
  errorHandler: async ({ task, error, type }) => {
    // type: 'execution' | 'timeout' | 'callback' | 'retry_exhausted'
    console.log(`任务 ${task.id} 失败 (${type}):`, error);
  },
  task: { /* ... */ }
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

```javascript
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

| 方法签名 | 说明 |
|----------|------|
| `updateProgress(progress)` | 更新任务进度 (0-100) |
| `polling(callback, options)` | 轮询外部服务直到完成 |
| `next(context)` | 设置任务为 waiting 状态，返回 `false` 暂停执行 |
| `log({ data, message })` | 记录任务执行日志 |

**polling options 参数：**

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `maxPollTimes` | number | `20` | 最大轮询次数 |
| `pollInterval` | number | `10000` | 轮询间隔（毫秒） |

**polling callback 返回格式：**

| 字段 | 类型 | 说明 |
|------|------|------|
| `result` | string | `'success'` / `'failed'` / `'pending'` |
| `data` | object | 成功时返回的数据 |
| `message` | string | 消息 |
| `progress` | number | 当前进度 |

#### 任务类型处理函数

任务完成后自动调用对应 `task[type]` 处理函数，接收参数：

| 参数名 | 类型 | 说明 |
|--------|------|------|
| `task` | Task | 任务实例 |
| `result` | object / string | 任务输出结果 |
| `context` | object | 任务上下文（`next` 时设置的数据） |

#### 运行时动态添加

```javascript
// 通过 append 方法运行时添加任务目录和类型
const result = await fastify.task.services.append({
  dirs: ['/path/to/tasks'],
  tasks: {
    'new-type': async ({ task, result }) => { /* 处理逻辑 */ }
  }
});
// result.dirs  → 实际添加的目录列表
// result.tasks → 实际添加的类型列表
```

#### 签名验证

当任务通过 `next({ secret: '密钥' })` 设置了密钥时，外部回调需提供 HMAC-SHA256 签名。

**签名生成方法：**

```javascript
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

| 接口 | data 格式 |
|------|-----------|
| `processNext` | `result` 字符串（JSON 格式结果） |
| `logWithSignature` | `{ data, message }` 对象 |
| `callbackWithSignature` | `{ code, data, message }` 对象 |

```javascript
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

| 依赖 | 说明 |
|------|------|
| `fastify-cron` | 定时任务支持 |
| `@kne/fastify-namespace` | 命名空间模块化 |
| `@kne/fastify-statistics` | 统计数据采集 |
| `fastify-sequelize` | 数据库模型支持 |

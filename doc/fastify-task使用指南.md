# fastify-task 使用指南

## 概述

`fastify-task` 是一个基于 Fastify 的任务调度插件，支持手动任务和系统自动任务的管理、执行和监控。

## 安装依赖

```bash
npm install fastify-task
```

## 插件注册

```javascript
const fastify = require('fastify')();

fastify.register(require('fastify-task'), {
  // 配置项
});
```

## 配置选项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `dbTableNamePrefix` | string | `'t_'` | 数据库表名前缀 |
| `prefix` | string | `'/api/task'` | API 路由前缀 |
| `name` | string | `'task'` | 插件命名空间名称 |
| `limit` | number | `10` | 系统任务并发执行上限 |
| `dir` | string | `'libs/tasks'` | 任务脚本目录 |
| `cronTime` | string | `'*/10 * * * *'` | 定时任务 Cron 表达式 |
| `scriptName` | string | `'index'` | 默认任务脚本名称 |
| `maxPollTimes` | number | `20` | 最大轮询次数 |
| `pollInterval` | number | `10000` | 轮询间隔（毫秒） |
| `getUserModel` | function | - | 获取用户 Model 的函数 |
| `getAuthenticate` | function | - | 获取认证中间件的函数 |
| `task` | object | `{}` | 任务类型处理函数配置 |

## API 接口

### 1. 获取任务列表

```
GET /api/task/list
```

**Query 参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `perPage` | number | 20 | 每页数量 |
| `currentPage` | number | 1 | 当前页码 |
| `filter` | object | - | 过滤条件 |
| `sort` | object | - | 排序条件 |

**filter 支持字段：**
- `id` - 任务ID
- `targetId` - 目标对象ID
- `targetName` - 目标名称（模糊匹配）
- `type` - 任务类型
- `status` - 任务状态
- `runnerType` - 执行者类型
- `createdAt` - 创建时间范围 `{startTime, endTime}`
- `completedAt` - 完成时间范围 `{startTime, endTime}`

### 2. 手动完成任务

```
POST /api/task/complete
```

**Body 参数：**

```json
{
  "id": "任务ID",
  "status": "success | failed",
  "error": "错误信息",
  "msg": "消息",
  "output": {}
}
```

### 3. 取消任务

```
POST /api/task/cancel
```

**Body 参数：**

```json
{
  "id": "任务ID"
}
```

### 4. 重试任务

```
POST /api/task/retry
```

**Body 参数：**

```json
{
  "id": "单个任务ID",
  "taskIds": ["批量任务ID数组"]
}
```

### 5. 处理任务 Next（等待回调）

```
POST /api/task/next
```

**Body 参数：**

```json
{
  "id": "任务ID",
  "signature": "签名",
  "result": "结果JSON字符串"
}
```

## 任务状态

| 状态 | 说明 |
|------|------|
| `pending` | 待处理 |
| `running` | 执行中 |
| `waiting` | 等待回调 |
| `success` | 成功 |
| `failed` | 失败 |
| `canceled` | 已取消 |

## 任务执行者类型

| 类型 | 说明 |
|------|------|
| `manual` | 手动执行（需调用 complete 接口完成） |
| `system` | 系统自动执行 |

## 服务方法

通过 `fastify.task.services` 访问：

```javascript
const { services } = fastify.task;

// 创建任务
await services.create({
  userId: '用户ID',
  input: { /* 输入数据 */ },
  type: '任务类型',
  targetId: '目标对象ID',
  targetType: '目标对象类型',
  runnerType: 'manual | system',
  delay: 0,  // 延迟执行秒数
  scriptName: 'index',  // 脚本名称
  options: {}  // 扩展选项
});

// 获取任务详情
await services.detail({ id: '任务ID' });

// 获取任务列表
await services.list({ filter, perPage, currentPage, sort });

// 取消任务
await services.cancel({ id: '任务ID' });

// 重试任务
await services.retry({ id: '任务ID' });

// 等待任务完成（轮询）
await services.waitingComplete({ 
  id: '任务ID',
  pollInterval: 1000,
  maxPollTimes: 20
});

// 手动触发任务执行
await services.runner();
```

## 任务脚本开发

在 `dir` 配置的目录下创建任务脚本：

```
libs/tasks/
└── <任务类型>/
    └── <scriptName>.js
```

**脚本模板：**

```javascript
module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
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
  });
  
  // 等待外部回调
  await next({
    secret: '签名密钥',
    // 其他上下文数据
  });
  
  return { /* 返回结果 */ };
};
```

### executor 辅助方法

| 方法 | 说明 |
|------|------|
| `updateProgress(progress)` | 更新任务进度 (0-100) |
| `polling(callback, options)` | 轮询外部服务直到完成 |
| `next(context)` | 设置任务为等待回调状态 |

### polling 配置

```javascript
await polling(callback, {
  maxPollTimes: 20,   // 最大轮询次数
  pollInterval: 10000 // 轮询间隔（毫秒）
});
```

**callback 返回格式：**

```javascript
{
  result: 'success | failed | pending',
  data: {},      // 成功时返回的数据
  message: '',   // 消息
  progress: 0    // 当前进度
}
```

## 任务类型处理函数

在配置中定义任务完成后的处理逻辑：

```javascript
fastify.register(require('fastify-task'), {
  task: {
    'export-excel': async ({ task, result, context }) => {
      // 任务成功完成后的业务处理
      console.log('导出任务完成:', result);
    },
    'send-email': async ({ task, result }) => {
      // 发送邮件完成后的处理
    }
  }
});
```

## 插件依赖

- `fastify-cron` - 定时任务支持
- `@kne/fastify-namespace` - 命名空间模块化
- `fastify-sequelize` - 数据库模型支持

## 数据模型

任务模型 `task` 包含以下字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `type` | STRING | 任务类型 |
| `scriptName` | STRING | 任务脚本名称 |
| `targetId` | STRING | 任务目标对象ID |
| `targetType` | STRING | 任务目标对象类型 |
| `runnerType` | ENUM | 执行者类型 (manual/system) |
| `startTime` | DATE | 任务最早执行时间 |
| `completedAt` | DATE | 任务完成时间 |
| `input` | JSON | 输入数据 |
| `output` | JSON | 输出数据 |
| `error` | JSON | 错误信息 |
| `status` | ENUM | 任务状态 |
| `context` | JSON | 上下文信息 |
| `pollResults` | JSON | 轮询执行结果 |
| `pollCount` | INTEGER | 轮询次数 |
| `progress` | INTEGER | 任务进度 (0-100) |
| `msg` | TEXT | 任务消息 |
| `options` | JSON | 任务扩展选项 |

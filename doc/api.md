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

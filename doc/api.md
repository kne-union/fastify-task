### 插件配置

| 属性名               | 类型       | 默认值            | 说明                   |
|-------------------|----------|----------------|----------------------|
| dbTableNamePrefix | string   | 't_'           | 数据库表名前缀              |
| prefix            | string   | '/api/task'    | API 路由前缀             |
| name              | string   | 'task'         | 命名空间名称               |
| limit             | number   | 10             | 并发执行任务数上限            |
| dir               | string   | 'libs/tasks'   | 任务脚本目录               |
| cronTime          | string   | '*/10 * * * *' | cron 执行周期，设为 null 禁用 |
| scriptName        | string   | 'index'        | 默认任务脚本名称             |
| maxPollTimes      | number   | 20             | 最大轮询次数               |
| pollInterval      | number   | 10000          | 轮询间隔（毫秒）             |
| getUserModel      | function | -              | 获取用户模型的函数            |
| getAuthenticate   | function | -              | 获取认证中间件的函数           |
| task              | object   | {}             | 任务类型处理函数映射           |

### HTTP API

#### 获取任务列表

**GET** `{prefix}/list`

| 参数名                          | 类型     | 默认值 | 说明         |
|------------------------------|--------|-----|------------|
| perPage                      | number | 20  | 每页数量       |
| currentPage                  | number | 1   | 当前页码       |
| filter.id                    | string | -   | 任务ID       |
| filter.targetId              | string | -   | 目标对象ID     |
| filter.targetName            | string | -   | 目标名称（模糊匹配） |
| filter.type                  | string | -   | 任务类型       |
| filter.status                | string | -   | 任务状态       |
| filter.runnerType            | string | -   | 执行者类型      |
| filter.createdAt.startTime   | string | -   | 创建时间起始     |
| filter.createdAt.endTime     | string | -   | 创建时间结束     |
| filter.completedAt.startTime | string | -   | 完成时间起始     |
| filter.completedAt.endTime   | string | -   | 完成时间结束     |
| sort                         | object | -   | 排序规则       |

**返回值**

| 属性名        | 类型     | 说明   |
|------------|--------|------|
| pageData   | array  | 任务列表 |
| totalCount | number | 总数量  |

#### 手动完成任务

**POST** `{prefix}/complete`

| 参数名    | 类型     | 必填 | 说明                |
|--------|--------|----|-------------------|
| id     | string | 是  | 任务ID              |
| status | string | 是  | 状态：success/failed |
| error  | string | 否  | 错误信息              |
| msg    | string | 否  | 消息                |
| output | object | 否  | 输出数据              |

#### 取消任务

**POST** `{prefix}/cancel`

| 参数名 | 类型     | 必填 | 说明   |
|-----|--------|----|------|
| id  | string | 是  | 任务ID |

#### 重试任务

**POST** `{prefix}/retry`

| 参数名     | 类型       | 必填 | 说明     |
|---------|----------|----|--------|
| id      | string   | 否  | 单个任务ID |
| taskIds | string[] | 否  | 任务ID数组 |

#### 处理任务 Next

**POST** `{prefix}/next`

| 参数名       | 类型     | 必填 | 说明                                     |
|-----------|--------|----|----------------------------------------|
| id        | string | 是  | 任务ID                                   |
| signature | string | 否  | HMAC-SHA256签名（当任务设置了context.secret时必填） |
| result    | string | 是  | JSON格式的结果                              |

#### 记录任务日志

**POST** `{prefix}/log`

| 参数名       | 类型     | 必填 | 说明                                     |
|-----------|--------|----|----------------------------------------|
| id        | string | 是  | 任务ID（body）                             |
| taskId    | string | 是  | 任务ID（query）                            |
| data      | object | 否  | 日志数据                                   |
| message   | string | 否  | 日志消息                                   |
| signature | string | 否  | HMAC-SHA256签名（当任务设置了context.secret时必填） |

#### 任务回调

**POST** `{prefix}/callback`

| 参数名       | 类型     | 必填 | 说明                                     |
|-----------|--------|----|----------------------------------------|
| id        | string | 是  | 任务ID（body）                             |
| taskId    | string | 是  | 任务ID（query）                            |
| code      | number | 是  | 状态码，0为成功                               |
| data      | object | 否  | 回调数据                                   |
| message   | string | 否  | 回调消息                                   |
| signature | string | 否  | HMAC-SHA256签名（当任务设置了context.secret时必填） |

### 服务层 API

#### create - 创建任务

| 参数名        | 类型     | 必填 | 说明                  |
|------------|--------|----|---------------------|
| userId     | string | 否  | 用户ID                |
| input      | object | 否  | 输入数据                |
| type       | string | 是  | 任务类型                |
| targetId   | string | 是  | 目标对象ID              |
| targetType | string | 是  | 目标对象类型              |
| runnerType | string | 否  | 执行者类型：manual/system |
| delay      | number | 否  | 延迟执行秒数              |
| scriptName | string | 否  | 脚本名称                |
| options    | object | 否  | 扩展选项                |

**返回值**：Task 实例

#### detail - 获取任务详情

| 参数名 | 类型     | 必填 | 说明   |
|-----|--------|----|------|
| id  | string | 是  | 任务ID |

**返回值**：Task 实例

#### list - 获取任务列表

同 HTTP API list 接口参数

#### complete - 完成任务

| 参数名    | 类型     | 必填 | 说明                |
|--------|--------|----|-------------------|
| id     | string | 是  | 任务ID              |
| userId | string | 否  | 完成人ID             |
| output | object | 否  | 输出数据              |
| status | string | 是  | 状态：success/failed |
| error  | string | 否  | 错误信息              |

#### cancel - 取消任务

| 参数名        | 类型     | 必填 | 说明           |
|------------|--------|----|--------------|
| id         | string | 否  | 任务ID         |
| targetId   | string | 否  | 目标对象ID（批量取消） |
| targetType | string | 否  | 目标对象类型（批量取消） |
| type       | string | 否  | 任务类型（批量取消）   |

#### retry - 重试任务

| 参数名     | 类型       | 必填 | 说明     |
|---------|----------|----|--------|
| id      | string   | 否  | 单个任务ID |
| taskIds | string[] | 否  | 任务ID数组 |

#### log - 记录日志（内部调用）

> 内部调用方法，不需要签名验证

| 参数名     | 类型     | 必填 | 说明              |
|-----------|--------|----|-----------------|
| id        | string | 是  | 任务ID            |
| taskId    | string | 是  | 任务ID（id的别名）     |
| data      | object | 否  | 日志数据            |
| message   | string | 否  | 日志消息            |

#### logWithSignature - 记录日志（外部调用）

> 外部调用方法，需要签名验证（HTTP API使用）

| 参数名       | 类型     | 必填 | 说明                                     |
|-----------|--------|----|----------------------------------------|
| id        | string | 是  | 任务ID                                   |
| taskId    | string | 是  | 任务ID（id的别名）                            |
| data      | object | 否  | 日志数据                                   |
| message   | string | 否  | 日志消息                                   |
| signature | string | 否  | HMAC-SHA256签名（当任务设置了context.secret时必填） |

#### callback - 任务回调（内部调用）

> 内部调用方法，不需要签名验证

| 参数名     | 类型     | 必填 | 说明        |
|-----------|--------|----|-----------|
| id        | string | 是  | 任务ID      |
| code      | number | 是  | 状态码，0为成功  |
| data      | object | 否  | 回调数据      |
| message   | string | 否  | 回调消息      |

#### callbackWithSignature - 任务回调（外部调用）

> 外部调用方法，需要签名验证（HTTP API使用）

| 参数名       | 类型     | 必填 | 说明                                     |
|-----------|--------|----|----------------------------------------|
| id        | string | 是  | 任务ID                                   |
| code      | number | 是  | 状态码，0为成功                               |
| data      | object | 否  | 回调数据                                   |
| message   | string | 否  | 回调消息                                   |
| signature | string | 否  | HMAC-SHA256签名（当任务设置了context.secret时必填） |

#### runner - 执行系统任务

无参数，由 cron 定时调用

#### resetAll - 重置所有运行中任务

无参数，将所有 running 状态任务重置为 pending

#### waitingComplete - 等待任务完成

| 参数名          | 类型     | 默认值  | 说明       |
|--------------|--------|------|----------|
| id           | string | -    | 任务ID     |
| pollInterval | number | 1000 | 轮询间隔（毫秒） |
| maxPollTimes | number | 20   | 最大轮询次数   |

**返回值**：任务输出数据

### 任务模型字段

| 属性名         | 类型     | 说明                  |
|-------------|--------|---------------------|
| id          | string | 任务ID                |
| type        | string | 任务类型                |
| scriptName  | string | 脚本名称                |
| targetId    | string | 目标对象ID              |
| targetType  | string | 目标对象类型              |
| runnerType  | enum   | 执行者类型：manual/system |
| startTime   | date   | 最早执行时间              |
| completedAt | date   | 完成时间                |
| input       | json   | 输入数据                |
| output      | json   | 输出数据                |
| error       | json   | 错误信息                |
| status      | enum   | 任务状态                |
| context     | json   | 上下文信息               |
| pollResults | json   | 轮询结果记录              |
| pollCount   | number | 轮询次数                |
| progress    | number | 进度(0-100)           |
| msg         | text   | 消息                  |
| options     | json   | 扩展选项                |

@kne/fastify-task 是一个 Fastify 插件，用于任务编排、追踪任务执行状态和结果。支持系统自动执行和手动执行两种模式，提供完整的任务生命周期管理。

### 主要特性

- **任务创建与管理**：支持创建、取消、重试、完成任务
- **多种执行模式**：支持系统自动执行（system）和手动执行（manual）
- **定时任务调度**：基于 cron 表达式的定时任务执行
- **任务状态追踪**：实时追踪任务进度、轮询结果
- **异步回调支持**：支持外部系统回调通知任务完成
- **日志记录**：支持任务执行日志记录，最多保留100条

### 使用场景

- 文档导出、数据迁移等异步处理任务
- 第三方 API 调用后异步回调处理
- 定时批量数据处理
- 需要追踪进度和状态的长时任务

### 任务状态流转

| 状态       | 说明     |
|----------|--------|
| pending  | 待执行    |
| running  | 执行中    |
| waiting  | 等待外部回调 |
| success  | 执行成功   |
| failed   | 执行失败   |
| canceled | 已取消    |

### 签名验证

当任务的 `context.secret` 设置了密钥时，外部调用需要提供 HMAC-SHA256 签名进行验证。

#### 签名生成方法

```javascript
const crypto = require('node:crypto');

function generateSignature({ secret, id, data }) {
  // data 可以是字符串或对象
  const dataStr = typeof data === 'string' ? data : JSON.stringify(data);
  const dataToSign = `${id}|${dataStr}`;
  const hmac = crypto.createHmac('sha256', secret);
  hmac.update(dataToSign);
  return hmac.digest('hex');
}
```

#### 各接口签名数据格式

| 接口                 | data 格式                        |
|----------------------|----------------------------------|
| processNext          | `result` 字符串（JSON格式结果）   |
| logWithSignature     | `{ data, message }` 对象         |
| callbackWithSignature| `{ code, data, message }` 对象   |

#### 示例

```javascript
// processNext 签名
const result = JSON.stringify({ code: 0, data: { output: 'done' } });
const signature = generateSignature({ secret: 'your-secret', id: 'task-1', data: result });

// logWithSignature 签名
const signature = generateSignature({
  secret: 'your-secret',
  id: 'task-1',
  data: { data: { key: 'value' }, message: '日志消息' }
});

// callbackWithSignature 签名
const signature = generateSignature({
  secret: 'your-secret',
  id: 'task-1',
  data: { code: 0, data: { result: 'done' }, message: '成功' }
});
```
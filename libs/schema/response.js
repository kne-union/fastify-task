// 响应 schema 只用于 OpenAPI 文档：字段可空、对象允许额外字段，避免序列化改变实际输出
const str = description => ({ type: 'string', nullable: true, description });
const num = description => ({ type: 'number', nullable: true, description });
// ID、日期、JSON 列可能是 number / string / Date / 任意结构，不声明类型，原样输出
const any = description => ({ description });
const obj = (description, properties = {}) => ({ type: 'object', nullable: true, additionalProperties: true, description, properties });
const list = (description, items) => ({ type: 'array', nullable: true, description, items });

const success = (description, schema) => ({ 200: Object.assign({}, schema || { type: 'object', additionalProperties: true }, { description }) });
const empty = description => success(description || '操作成功，返回空对象');

const TASK_STATUS = '任务状态：pending 待执行，running 执行中，waiting 等待外部回调/轮询，success 成功，failed 失败，canceled 已取消';
const RUNNER_TYPE = '执行方式：manual 手动执行，system 系统自动执行';

const task = obj('任务', {
  id: any('任务 ID'),
  type: str('任务类型'),
  scriptName: str('任务脚本名称'),
  targetId: any('任务目标对象 ID'),
  targetType: str('任务目标对象类型'),
  runnerType: str(RUNNER_TYPE),
  priority: num('优先级，数值越大越优先'),
  parentTaskId: any('父任务 ID'),
  retryCount: num('已自动重试次数'),
  maxRetries: num('最大自动重试次数，0 表示不自动重试'),
  timeout: num('任务超时时间（毫秒），0 表示不超时'),
  startTime: any('任务最早执行时间'),
  startedAt: any('实际开始执行时间'),
  completedAt: any('完成时间'),
  userId: any('创建人用户 ID，系统创建为空'),
  completedUserId: any('完成任务的用户 ID'),
  input: any('输入数据'),
  output: any('输出数据'),
  error: any('错误信息'),
  status: str(TASK_STATUS),
  context: any('执行上下文，如回调签名密钥等'),
  pollResults: any('轮询执行结果列表'),
  pollCount: num('已轮询次数'),
  progress: num('任务进度（0-100）'),
  msg: str('任务消息或错误信息'),
  options: any('扩展选项，logs 为最近 100 条任务日志（新的在前）'),
  createdAt: any('创建时间'),
  updatedAt: any('更新时间')
});

const taskList = {
  type: 'object',
  additionalProperties: true,
  properties: {
    pageData: list('当前页任务列表，默认按创建时间倒序', task),
    totalCount: num('符合条件的任务总数')
  }
};

const DURATION_ITEM = '每项为 { count, avgWaitingTime, avgExecutionTime, avgTotalTime }';

const bucket = obj('趋势桶', {
  key: str('桶 key：YYYY-MM-DD（日 / 周起始日）或 YYYY-MM（月）'),
  start: any('桶起点（含）'),
  end: any('桶终点（不含）；最后一个桶为当前时间')
});

const duration = obj('范围内耗时汇总；查询异常时为 null', {
  completedCount: num('已结束任务数'),
  successCount: num('成功任务数'),
  failedCount: num('失败任务数'),
  canceledCount: num('取消任务数'),
  avgWaitingTime: num('平均等待时间（毫秒），无耗时数据时为 0'),
  avgExecutionTime: num('平均执行时间（毫秒），无耗时数据时为 0'),
  avgTotalTime: num('平均总耗时（毫秒），无耗时数据时为 0'),
  byType: obj(`按任务类型拆分，键为任务类型，${DURATION_ITEM}`),
  byRunnerType: obj(`按执行方式拆分，键为 manual / system，${DURATION_ITEM}`),
  byTypeByRunnerType: obj(`先按执行方式再按任务类型拆分，结构为 { [runnerType]: { [type]: 耗时项 } }，${DURATION_ITEM}`)
});

const statistics = {
  type: 'object',
  additionalProperties: true,
  properties: {
    range: str('时间范围：7d / 1m / 3m / 1y'),
    rangeLabel: str('时间范围名称，如「近7天」'),
    granularity: str('趋势粒度：7d / 1m 为 day，3m 为 week（周一起），1y 为 month'),
    startTime: any('统计起始时间（第一个桶的起点）'),
    endTime: any('统计截止时间（当前时间）'),
    buckets: list('趋势桶列表，按时间正序', bucket),
    totalTasks: num('范围内已结束（成功、失败、取消）的任务总数'),
    byStatus: any('按状态统计的任务数，键为 success / failed / canceled'),
    byType: any('按任务类型统计的任务数，键为任务类型'),
    byRunnerType: any('按执行方式统计的任务数，键为 manual / system'),
    recentTrend: any('按桶趋势，每项为 { date, count }，date 为桶 key，无数据的桶补 0'),
    recentTrendByStatus: any('按桶按状态趋势，每项为 { date, status, count }，每个桶都含 success / failed / canceled'),
    recentTrendByType: any('按桶按类型趋势，每项为 { date, type, count }'),
    durationTrend: any('按桶耗时趋势，每项含 date（桶 key）、完成/成功/失败数及平均等待、执行、总耗时（毫秒）'),
    duration,
    hourlyTrend: any('按小时趋势，每项为 { date, hour, total, success, failed, canceled }；查询失败或无数据时不返回'),
    hourlyCompletionTrend: any('按小时、类型、执行方式的完成明细，每项含 date、hour、type、runnerType 及完成/成功/失败/取消数')
  }
};

module.exports = { str, num, any, obj, list, success, empty, TASK_STATUS, RUNNER_TYPE, task, taskList, statistics };

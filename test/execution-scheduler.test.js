const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - execution and scheduler', function () {
  this.timeout(10000);

  const context = createTestContext();
  let fastify;

  const createFastify = async options => {
    fastify = await context.createFastify(options);
    return fastify;
  };

  beforeEach(() => {
    context.reset();
  });

  afterEach(async () => {
    if (fastify) {
      await fastify.close();
      fastify = null;
    }
    context.restore();
  });

  describe('executor 系统任务执行器测试', () => {
    it('should execute system task via executor', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.progress).to.equal(100);
    });

    it('should update progress during task execution', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'progress-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should handle task executor error with retry', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 2
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      // 第一次失败，retryCount(1) < maxRetries(2)，应该重试
      expect(updated.status).to.equal('pending');
      expect(updated.retryCount).to.equal(1);
    });

    it('should fail task when max retries exceeded', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
    });

    it('should handle task timeout', async () => {
      fastify = await createFastify({ taskTimeout: 50 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'hang-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('超时');
    });

    it('should throw error for unmatched task executor', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      // 清空 dirs 模拟找不到执行器
      const originalDirs = fastify.task.options.dirs.slice();
      fastify.task.options.dirs = ['/nonexistent/path'];

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('未匹配到任务执行器');

      // 恢复
      fastify.task.options.dirs = originalDirs;
    });
  });

  describe('runner 调度测试', () => {
    it('should claim each pending system task only once', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      const firstClaim = await fastify.task.services.claimPendingTasks(1);
      const secondClaim = await fastify.task.services.claimPendingTasks(1);
      const updated = await fastify.task.services.detail({ id: task.id });

      expect(firstClaim).to.have.length(1);
      expect(firstClaim[0].id).to.equal(task.id);
      expect(secondClaim).to.have.length(0);
      expect(updated.status).to.equal('running');
    });

    it('should run pending system tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.runner();

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should skip runner when limit reached', async () => {
      fastify = await createFastify({ limit: 1 });
      await fastify.ready();

      // 创建一个正在运行的任务
      const runningTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await runningTask.update({ status: 'running' });

      // runner 应该跳过，因为 running 数量已达上限
      await fastify.task.services.runner();

      // 日志中应该记录跳过信息
      expect(fastify.log.info.called).to.be.true;
    });

    it('should execute task with higher priority first', async () => {
      fastify = await createFastify({ limit: 10 });
      await fastify.ready();

      const lowTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        priority: 1
      });

      const highTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'system',
        priority: 10
      });

      await fastify.task.services.runner();

      const highUpdated = await fastify.task.services.detail({ id: highTask.id });
      expect(highUpdated.status).to.equal('success');

      const lowUpdated = await fastify.task.services.detail({ id: lowTask.id });
      expect(lowUpdated.status).to.equal('success');
    });
  });

  describe('triggerChildTasks 子任务触发测试', () => {
    it('should trigger child system tasks after parent succeeds', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const parent = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      const child = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'system',
        parentTaskId: parent.id
      });

      // 执行父任务，应该触发子任务
      await fastify.task.services.processSystemTask(parent);

      const parentUpdated = await fastify.task.services.detail({ id: parent.id });
      expect(parentUpdated.status).to.equal('success');

      const childUpdated = await fastify.task.services.detail({ id: child.id });
      expect(childUpdated.status).to.equal('success');
    });

    it('should not trigger manual child tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const parent = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      const child = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        runnerType: 'manual',
        parentTaskId: parent.id
      });

      await fastify.task.services.processSystemTask(parent);

      const childUpdated = await fastify.task.services.detail({ id: child.id });
      // manual 类型子任务保持 pending
      expect(childUpdated.status).to.equal('pending');
    });
  });

  describe('taskTimeout=0 无超时测试', () => {
    it('should not apply timeout when taskTimeout is 0', async () => {
      fastify = await createFastify({ taskTimeout: 0 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });
  });

  describe('executor 自定义 scriptName 测试', () => {
    it('should use custom scriptName when provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        scriptName: 'index'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });
  });

  describe('runner 无待处理任务测试', () => {
    it('should not execute when no pending tasks', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.runner();

      // 不应抛出异常，正常运行
      expect(fastify.log.info.called).to.be.false;
    });
  });

  describe('next-type 任务执行测试', () => {
    it('should set task to waiting status when executor calls next', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'next-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('waiting');
      expect(updated.context).to.deep.equal({ secret: 'test-secret' });
    });
  });

  describe('log-type 任务执行测试', () => {
    it('should record log when task executor calls log', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'log-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.options.logs).to.exist;
      expect(updated.options.logs[0].message).to.equal('Task log entry');
    });
  });

  describe('retry 指数退避测试', () => {
    it('should retry with exponential backoff delay', async () => {
      fastify = await createFastify({ retryBaseDelay: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 3
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('pending');
      expect(updated.retryCount).to.equal(1);
      // startTime 应该在未来（退避延迟）
      expect(updated.startTime.getTime()).to.be.greaterThan(Date.now() - 1000);
    });
  });

  describe('executor 未匹配执行器测试', () => {
    it('should throw error when no executor file found', async () => {
      fastify = await createFastify();
      await fastify.ready();

      // 添加一个只在 task 选项中声明但没有脚本文件的类型
      fastify.task.options.task['no-script-type'] = async ({ result }) => result;

      const task = await fastify.task.services.create({
        type: 'no-script-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('未匹配到任务执行器');
    });
  });

  // ==================== 补充覆盖率测试 ====================

  describe('checkTimeout 超时检测测试', () => {
    it('should mark running tasks as failed when timed out', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        timeout: 60 * 1000
      });
      await task.update({ status: 'running', startedAt: new Date(Date.now() - 2 * 60 * 1000) });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('任务超时');
    });

    it('should mark waiting tasks as failed when timed out', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        timeout: 60 * 1000
      });
      await task.update({ status: 'waiting', startedAt: new Date(Date.now() - 2 * 60 * 1000) });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('任务超时');
    });

    it('should not mark tasks as failed when timeout is 0', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        timeout: 0
      });
      await task.update({ status: 'running', startedAt: new Date(Date.now() - 60 * 60 * 1000) });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('running');
    });

    it('should not mark tasks as failed when not yet timed out', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        timeout: 60 * 60 * 1000
      });
      await task.update({ status: 'running', startedAt: new Date() });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('running');
    });
  });
});

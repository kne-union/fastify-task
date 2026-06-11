const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - core services', function () {
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

  describe('服务功能测试 - create', () => {
    it('should create task successfully', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        userId: 'user-1',
        input: { name: 'test' },
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      expect(task).to.exist;
      expect(task.type).to.equal('test-type');
      expect(task.status).to.equal('pending');
      expect(task.targetId).to.equal('target-1');
    });

    it('should auto-register and create task when type is not explicitly declared', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'undefined-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task).to.exist;
      expect(task.type).to.equal('undefined-type');
      expect(task.status).to.equal('pending');
    });

    it('should set delayed start time when delay is provided', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const delay = 60;
      const beforeCreate = Date.now();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        delay
      });

      expect(task.startTime.getTime()).to.be.greaterThan(beforeCreate + 1000 * delay - 100);
    });
  });

  describe('服务功能测试 - detail', () => {
    it('should return task by id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task).to.exist;
      expect(task.id).to.equal(created.id);
    });

    it('should throw error when task not found', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.detail({ id: 'non-existent' });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('任务不存在');
      }
    });
  });

  describe('服务功能测试 - list', () => {
    it('should return paginated task list', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image'
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1
      });

      expect(result.pageData).to.exist;
      expect(result.totalCount).to.equal(2);
    });

    it('should filter tasks by type', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      // 直接创建一个不同类型的任务到数据库，绕过 create 的类型验证
      const otherTask = await fastify.task.models.task.create({
        type: 'other-type',
        targetId: 'target-2',
        targetType: 'image',
        status: 'pending'
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { type: 'test-type' }
      });

      expect(result.totalCount).to.equal(1);
    });

    it('should filter tasks by status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image'
      });
      await task2.update({ status: 'success' });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { status: 'success' }
      });

      expect(result.totalCount).to.equal(1);
    });
  });

  describe('服务功能测试 - cancel', () => {
    it('should cancel task by id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('canceled');
    });

    it('should not cancel completed task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'success' });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
    });
  });

  describe('服务功能测试 - complete', () => {
    it('should complete task with success status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { result: 'done' },
        userId: 'user-1'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('success');
      expect(task.progress).to.equal(100);
    });

    it('should complete task with failed status', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'failed',
        error: 'Something went wrong',
        userId: 'user-1'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('服务功能测试 - retry', () => {
    it('should retry failed task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'failed' });

      await fastify.task.services.retry({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('pending');
    });

    it('should throw error when retrying non-failed task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      try {
        await fastify.task.services.retry({ id: created.id });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('只有失败或取消的任务允许重试');
      }
    });
  });

  describe('服务功能测试 - resetAll', () => {
    it('should reset all running tasks to pending', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task1 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document'
      });
      await task1.update({ status: 'running' });
      await task2.update({ status: 'running' });

      await fastify.task.services.resetAll();

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { status: 'pending' }
      });

      expect(result.totalCount).to.equal(2);
    });

    it('should reset only stale running tasks when staleOnly is true', async () => {
      fastify = await createFastify({ recoverRunningTaskAfter: 60 * 1000 });
      await fastify.ready();

      const staleTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      const activeTask = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document'
      });
      await staleTask.update({ status: 'running', startedAt: new Date(Date.now() - 2 * 60 * 1000) });
      await activeTask.update({ status: 'running', startedAt: new Date() });

      const affectedCount = await fastify.task.services.resetAll({ staleOnly: true });

      const staleUpdated = await fastify.task.services.detail({ id: staleTask.id });
      const activeUpdated = await fastify.task.services.detail({ id: activeTask.id });
      expect(affectedCount).to.equal(1);
      expect(staleUpdated.status).to.equal('pending');
      expect(activeUpdated.status).to.equal('running');
    });
  });

  describe('服务功能测试 - log', () => {
    it('should add log to task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.log({
        taskId: created.id,
        message: 'Test log message',
        data: { key: 'value' }
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs).to.exist;
      expect(task.options.logs[0].message).to.equal('Test log message');
    });
  });

  describe('边界情况测试', () => {
    it('should handle null input gracefully', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        input: null
      });

      expect(task).to.exist;
      expect(task.input).to.be.null;
    });

    it('should handle empty input object', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        input: {}
      });

      expect(task).to.exist;
      expect(task.input).to.deep.equal({});
    });
  });
});

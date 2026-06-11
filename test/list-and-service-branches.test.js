const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - list filters and service branches', function () {
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

  describe('startedAt 字段测试', () => {
    it('should set startedAt when complete is called', async () => {
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
      expect(task.startedAt).to.exist;
      expect(task.startedAt).to.be.instanceOf(Date);
    });

    it('should set startedAt when complete with failed status', async () => {
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
      expect(task.startedAt).to.exist;
    });

    it('should set completedAt when cancel is called', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.completedAt).to.exist;
      expect(task.status).to.equal('canceled');
    });
  });

  describe('list 过滤与排序测试', () => {
    it('should filter by createdAt range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const now = new Date().toISOString();
      const past = new Date(Date.now() - 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { createdAt: { startTime: past, endTime: now } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by createdAt startTime only', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const past = new Date(Date.now() - 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { createdAt: { startTime: past } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by createdAt endTime only', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      const future = new Date(Date.now() + 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { createdAt: { endTime: future } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should filter by completedAt range', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'success', completedAt: new Date() });

      const now = new Date().toISOString();
      const past = new Date(Date.now() - 86400000).toISOString();
      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        filter: { completedAt: { startTime: past, endTime: now } }
      });
      expect(result.totalCount).to.equal(1);
    });

    it('should sort by custom field', async () => {
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
        currentPage: 1,
        sort: { targetId: 'ASC' }
      });
      expect(result.totalCount).to.equal(2);
    });

    it('should filter by targetName from bracket query string', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        input: { name: 'Royal Caribbean Employee' }
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        input: { name: 'Other Employee' }
      });

      const response = await fastify.inject({
        method: 'GET',
        url: '/api/task/list?filter%5BtargetName%5D=Royal+Caribbean+Employee&perPage=20'
      });
      const result = JSON.parse(response.payload);

      expect(response.statusCode).to.equal(200);
      expect(result.totalCount).to.equal(1);
      expect(result.pageData[0].input.name).to.equal('Royal Caribbean Employee');
    });
  });

  describe('retry 批量测试', () => {
    it('should retry multiple tasks by taskIds', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task1 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await task1.update({ status: 'failed' });

      const task2 = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'image'
      });
      await task2.update({ status: 'canceled' });

      await fastify.task.services.retry({ taskIds: [task1.id, task2.id] });

      const t1 = await fastify.task.services.detail({ id: task1.id });
      expect(t1.status).to.equal('pending');

      const t2 = await fastify.task.services.detail({ id: task2.id });
      expect(t2.status).to.equal('pending');
    });
  });

  describe('log 超长截断测试', () => {
    it('should truncate logs when exceeding 100 entries', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      // 添加101条日志
      for (let i = 0; i < 101; i++) {
        await fastify.task.services.log({
          taskId: created.id,
          message: `Log ${i}`
        });
      }

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.options.logs.length).to.equal(100);
    });
  });

  describe('cancel 批量操作测试', () => {
    it('should batch cancel by targetId+targetType+type', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      const affectedCount = await fastify.task.services.cancel({
        targetId: 'target-1',
        targetType: 'document',
        type: 'test-type'
      });

      expect(affectedCount).to.equal(2);
    });
  });

  describe('complete 异常路径测试', () => {
    it('should fail task when task handler throws error', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'manual'
      });

      // 让 task handler 抛出异常
      fastify.task.options.task['test-type'] = async () => {
        throw new Error('Handler error');
      };

      try {
        await fastify.task.services.complete({
          id: created.id,
          status: 'success',
          output: { result: 'done' },
          userId: 'user-1'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('Handler error');
      }

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('complete 成功路径中 task handler 接收 result 测试', () => {
    it('should pass output as result to task handler on success', async () => {
      fastify = await createFastify();
      await fastify.ready();

      let handlerResult = null;
      fastify.task.options.task['test-type'] = async ({ task, result }) => {
        handlerResult = result;
        return result;
      };

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.complete({
        id: created.id,
        status: 'success',
        output: { data: 'test-output' },
        userId: 'user-1'
      });

      expect(handlerResult).to.deep.equal({ data: 'test-output' });
    });
  });

  describe('create 校验测试', () => {
    it('should throw error for negative delay', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          delay: -1
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('delay 必须为非负数');
      }
    });

    it('should throw error for non-numeric delay', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          delay: 'abc'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('delay 必须为非负数');
      }
    });

    it('should throw error for non-numeric priority', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          priority: 'high'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('priority 必须为整数');
      }
    });

    it('should throw error for non-integer maxRetries', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          maxRetries: 1.5
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('maxRetries 必须为非负整数');
      }
    });

    it('should throw error for unsafe scriptName', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          scriptName: '../index'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('scriptName 只能包含');
      }
    });
  });

  describe('cancel running 任务测试', () => {
    it('should cancel running task by id', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'running' });

      await fastify.task.services.cancel({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('canceled');
    });
  });

  describe('retry canceled 任务测试', () => {
    it('should retry canceled task', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'canceled' });

      await fastify.task.services.retry({ id: created.id });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('pending');
    });
  });

  describe('cancel 批量无匹配测试', () => {
    it('should return 0 when no tasks match bulk cancel criteria', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const affectedCount = await fastify.task.services.cancel({
        targetId: 'nonexistent',
        targetType: 'document',
        type: 'test-type'
      });

      expect(affectedCount).to.equal(0);
    });
  });

  describe('append 警告测试', () => {
    it('should warn when appending non-existent dir', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const result = await fastify.task.services.append({
        dirs: ['/nonexistent/path']
      });

      expect(result.dirs).to.deep.equal(['/nonexistent/path']);
      expect(fastify.task.options.dirs).to.include('/nonexistent/path');
    });
  });

  describe('list 自定义排序测试', () => {
    it('should list tasks with custom sort', async () => {
      fastify = await createFastify();
      await fastify.ready();

      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        priority: 5
      });
      await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-2',
        targetType: 'document',
        priority: 10
      });

      const result = await fastify.task.services.list({
        perPage: 10,
        currentPage: 1,
        sort: { priority: 'DESC' }
      });

      expect(result.totalCount).to.equal(2);
      expect(result.pageData[0].priority).to.equal(10);
      expect(result.pageData[1].priority).to.equal(5);
    });

    it('should reject unsupported sort field', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.list({
          sort: { 'input.name': 'ASC' }
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('不支持的排序字段');
      }
    });

    it('should reject unsupported sort direction', async () => {
      fastify = await createFastify();
      await fastify.ready();

      try {
        await fastify.task.services.list({
          sort: { priority: 'SIDEWAYS' }
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('不支持的排序方向');
      }
    });
  });

  describe('create timeout 校验测试', () => {
    it('should default timeout to 3600000 milliseconds', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      expect(task.timeout).to.equal(60 * 60 * 1000);
    });

    it('should throw error for non-integer timeout', async () => {
      fastify = await createFastify();
      await fastify.ready();
      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          timeout: 1.5
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('timeout 必须为非负整数');
      }
    });

    it('should throw error for negative timeout', async () => {
      fastify = await createFastify();
      await fastify.ready();
      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          timeout: -1
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('timeout 必须为非负整数');
      }
    });

    it('should throw error for non-numeric timeout', async () => {
      fastify = await createFastify();
      await fastify.ready();
      try {
        await fastify.task.services.create({
          type: 'test-type',
          targetId: 'target-1',
          targetType: 'document',
          timeout: 'abc'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('timeout 必须为非负整数');
      }
    });
  });
});

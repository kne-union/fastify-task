const { expect } = require('chai');
const sinon = require('sinon');
const path = require('node:path');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - polling and waiting complete', function () {
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

  describe('waitingComplete 测试', () => {
    it('should resolve when task completes via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      // waitingComplete 会自动执行 pending 的系统任务
      const result = await fastify.task.services.waitingComplete({
        id: task.id,
        pollInterval: 10,
        maxPollTimes: 50
      });

      expect(result).to.deep.equal({ result: 'success' });
    });

    it('should reject when task fails via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      try {
        await fastify.task.services.waitingComplete({
          id: task.id,
          pollInterval: 10,
          maxPollTimes: 50
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Task execution failed');
      }
    });

    it('should reject when task is canceled via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 设置为 running，避免 waitingComplete 自动执行
      await task.update({ status: 'running' });

      const completePromise = fastify.task.services.waitingComplete({
        id: task.id,
        pollInterval: 10,
        maxPollTimes: 50
      });

      setTimeout(async () => {
        await task.update({ status: 'canceled' });
      }, 50);

      try {
        await completePromise;
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('取消');
      }
    });

    it('should reject on timeout via waitingComplete', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 设置为 running，避免 waitingComplete 自动执行
      await task.update({ status: 'running' });

      try {
        await fastify.task.services.waitingComplete({
          id: task.id,
          pollInterval: 10,
          maxPollTimes: 2
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('任务超时');
      }
    });
  });

  describe('polling 轮询功能测试', () => {
    it('should execute polling-type task via processSystemTask', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.pollCount).to.be.greaterThan(0);
    });

    it('should handle polling with progress update', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should handle polling with custom options', async () => {
      fastify = await createFastify({ pollInterval: 10, maxPollTimes: 5 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
    });

    it('should handle polling with failed result', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-fail-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('任务处理失败');
    });

    it('should handle polling with pending then success result', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'polling-pending-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('success');
      expect(updated.pollCount).to.be.greaterThan(0);
    });

    it('should handle polling timeout', async () => {
      fastify = await createFastify({ pollInterval: 10, maxPollTimes: 1 });
      await fastify.ready();

      // 添加一个永远返回 pending 的任务类型
      const pendingPollingHandler = async (fastify, options, { polling }) => {
        return await polling(async () => {
          return { result: 'pending' };
        });
      };

      // 直接使用 append 添加任务
      await fastify.task.services.append({
        tasks: {
          'always-pending-type': async ({ task, result }) => result
        }
      });

      // 创建任务脚本目录
      const fs = require('fs-extra');
      const tempDir = path.resolve(__dirname, './tasks/always-pending-type');
      await fs.ensureDir(tempDir);
      await fs.writeFile(path.resolve(tempDir, 'index.js'), `module.exports = async (fastify, options, { polling }) => {\n  return await polling(async () => {\n    return { result: 'pending' };\n  });\n};\n`);

      const task = await fastify.task.services.create({
        type: 'always-pending-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('轮询超时');

      // 清理临时文件
      await fs.remove(tempDir);
    });

    it('should handle polling callback throwing error', async () => {
      fastify = await createFastify({ pollInterval: 10 });
      await fastify.ready();

      // 添加一个在 polling 中抛出异常的任务类型
      await fastify.task.services.append({
        tasks: {
          'polling-error-type': async ({ task, result }) => result
        }
      });

      const fs = require('fs-extra');
      const tempDir = path.resolve(__dirname, './tasks/polling-error-type');
      await fs.ensureDir(tempDir);
      await fs.writeFile(path.resolve(tempDir, 'index.js'), `module.exports = async (fastify, options, { polling }) => {\n  return await polling(async () => {\n    throw new Error('Polling callback error');\n  });\n};\n`);

      const task = await fastify.task.services.create({
        type: 'polling-error-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system',
        maxRetries: 0
      });

      await fastify.task.services.processSystemTask(task);

      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('Polling callback error');

      // 清理临时文件
      await fs.remove(tempDir);
    });
  });

  describe('waitingComplete 异常捕获测试', () => {
    it('should reject when reload throws error', async () => {
      fastify = await createFastify();
      await fastify.ready();

      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      // 设置为 running，避免 waitingComplete 自动执行
      await task.update({ status: 'running' });

      // 覆盖 reload 使其抛出异常
      const originalReload = task.reload;
      task.reload = async () => {
        throw new Error('Reload failed');
      };

      try {
        await fastify.task.services.waitingComplete({
          id: task.id,
          pollInterval: 10,
          maxPollTimes: 10
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.equal('Reload failed');
      }

      // 恢复
      task.reload = originalReload;
    });
  });

  describe('waitingComplete 即时状态检查测试', () => {
    it('should reject when task is already canceled before call', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await task.update({ status: 'canceled' });
      try {
        await fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 5 });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('取消');
      }
    });

    it('should reject when task is already failed before call', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await task.update({ status: 'failed', error: 'Already failed' });
      try {
        await fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 5 });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Already failed');
      }
    });

    it('should resolve when task is already successful before call', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await task.update({ status: 'success', output: { data: 'done' } });
      const result = await fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 5 });
      expect(result).to.deep.equal({ data: 'done' });
    });
  });

  describe('waitingComplete 轮询路径测试', () => {
    it('should reject when task becomes failed during polling', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await task.update({ status: 'running' });
      const promise = fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 50 });
      setTimeout(async () => {
        await task.update({ status: 'failed', error: 'Polling failed' });
      }, 30);
      try {
        await promise;
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Polling failed');
      }
    });

    it('should reject with default message when task fails without error', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await task.update({ status: 'running', error: null });
      const promise = fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 50 });
      setTimeout(async () => {
        await task.update({ status: 'failed' });
      }, 30);
      try {
        await promise;
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('失败');
      }
    });

    it('should resolve when task becomes success during polling', async () => {
      fastify = await createFastify();
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document',
        runnerType: 'system'
      });
      await task.update({ status: 'running' });
      const promise = fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 50 });
      setTimeout(async () => {
        await task.update({ status: 'success', output: { value: 42 } });
      }, 30);
      const result = await promise;
      expect(result).to.deep.equal({ value: 42 });
    });
  });
});

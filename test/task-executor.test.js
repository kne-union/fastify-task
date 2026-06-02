const { expect } = require('chai');
const sinon = require('sinon');
const { createFastify } = require('./helpers/setup');

const path = require('node:path');

describe('@kne/fastify-task - 执行器与轮询', function () {
  this.timeout(10000);
  let fastify;
  let taskData = [];
  let taskIdCounter = { value: 1 };
  beforeEach(() => { taskData = []; taskIdCounter.value = 1; });
  afterEach(async () => { if (fastify) { await fastify.close(); fastify = null; } sinon.restore(); });

  describe('cancel 批量操作测试', () => {
    it('should batch cancel by targetId+targetType+type', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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

  describe('processNext 错误路径测试', () => {
    it('should fail task when processNext receives non-zero code', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });
      await created.update({ status: 'waiting', context: {} });

      const resultStr = JSON.stringify({ code: 1, message: 'External error' });
      await fastify.task.services.processNext({
        id: created.id,
        result: resultStr
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('create 校验测试', () => {
    it('should throw error for negative delay', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
        expect(e.message).to.include('参数必须在 0 到 86400 之间');
      }
    });

    it('should throw error for non-numeric delay', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
        expect(e.message).to.include('参数必须为有效数字');
      }
    });

    it('should throw error for non-numeric priority', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
        expect(e.message).to.include('参数必须为有效数字');
      }
    });

    it('should throw error for non-integer maxRetries', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
        expect(e.message).to.include('参数必须为整数');
      }
    });
  });

  describe('processNext 非等待状态测试', () => {
    it('should throw error when processNext is called on non-waiting task', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      try {
        await fastify.task.services.processNext({
          id: created.id,
          result: JSON.stringify({ code: 0, data: {} })
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('任务状态不允许此操作');
      }
    });
  });

  describe('next-type 任务执行测试', () => {
    it('should set task to waiting status when executor calls next', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({ retryBaseDelay: 10 }, taskData, taskIdCounter);
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

  describe('cancel running 任务测试', () => {
    it('should cancel running task by id', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
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

  describe('callback 非0 code 测试', () => {
    it('should complete with failed status when callback code is non-zero', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const created = await fastify.task.services.create({
        type: 'test-type',
        targetId: 'target-1',
        targetType: 'document'
      });

      await fastify.task.services.callback({
        id: created.id,
        code: 1,
        data: null,
        message: 'Error occurred'
      });

      const task = await fastify.task.services.detail({ id: created.id });
      expect(task.status).to.equal('failed');
    });
  });

  describe('cancel 批量无匹配测试', () => {
    it('should return 0 when no tasks match bulk cancel criteria', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();

      const result = await fastify.task.services.append({
        dirs: ['/nonexistent/path']
      });

      expect(result.dirs).to.deep.equal(['/nonexistent/path']);
      expect(fastify.task.options.dirs).to.include('/nonexistent/path');
    });
  });
  describe('polling 轮询功能测试', () => {
    it('should execute polling-type task via processSystemTask', async () => {
      fastify = await createFastify({ pollInterval: 10 }, taskData, taskIdCounter);
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
      fastify = await createFastify({ pollInterval: 10 }, taskData, taskIdCounter);
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
      fastify = await createFastify({ pollInterval: 10, maxPollTimes: 5 }, taskData, taskIdCounter);
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
      fastify = await createFastify({ pollInterval: 10 }, taskData, taskIdCounter);
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
      expect(updated.error).to.include('Polling task failed');
    });

    it('should handle polling with pending then success result', async () => {
      fastify = await createFastify({ pollInterval: 10 }, taskData, taskIdCounter);
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
      fastify = await createFastify({ pollInterval: 10, maxPollTimes: 1 }, taskData, taskIdCounter);
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
      await fs.writeFile(path.resolve(tempDir, 'index.js'),
        `module.exports = async (fastify, options, { polling }) => {\n  return await polling(async () => {\n    return { result: 'pending' };\n  });\n};\n`
      );

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
      expect(updated.error).to.include('超时');

      // 清理临时文件
      await fs.remove(tempDir);
    });

    it('should handle polling callback throwing error', async () => {
      fastify = await createFastify({ pollInterval: 10 }, taskData, taskIdCounter);
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
      await fs.writeFile(path.resolve(tempDir, 'index.js'),
        `module.exports = async (fastify, options, { polling }) => {\n  return await polling(async () => {\n    throw new Error('Polling callback error');\n  });\n};\n`
      );

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
      fastify = await createFastify({}, taskData, taskIdCounter);
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

  describe('complete 成功路径中 task handler 接收 result 测试', () => {
    it('should pass output as result to task handler on success', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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

  describe('executor 未匹配执行器测试', () => {
    it('should throw error when no executor file found', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
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

  describe('create timeout 校验测试', () => {
    it('should throw error for non-integer timeout', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.create({
          type: 'test-type', targetId: 'target-1', targetType: 'document', timeout: 1.5
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('参数必须为整数');
      }
    });

    it('should throw error for negative timeout', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.create({
          type: 'test-type', targetId: 'target-1', targetType: 'document', timeout: -1
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('参数必须在 0 到 1440 之间');
      }
    });

    it('should throw error for non-numeric timeout', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      try {
        await fastify.task.services.create({
          type: 'test-type', targetId: 'target-1', targetType: 'document', timeout: 'abc'
        });
        throw new Error('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('参数必须为有效数字');
      }
    });
  });

  describe('checkTimeout 超时检测测试', () => {
    it('should mark running tasks as failed when timed out', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', timeout: 1
      });
      await task.update({ status: 'running', startedAt: new Date(Date.now() - 2 * 60 * 1000) });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('任务超时');
    });

    it('should mark waiting tasks as failed when timed out', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', timeout: 1
      });
      await task.update({ status: 'waiting', startedAt: new Date(Date.now() - 2 * 60 * 1000) });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('failed');
      expect(updated.error).to.include('任务超时');
    });

    it('should not mark tasks as failed when timeout is 0', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', timeout: 0
      });
      await task.update({ status: 'running', startedAt: new Date(Date.now() - 60 * 60 * 1000) });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('running');
    });

    it('should not mark tasks as failed when not yet timed out', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document',
        runnerType: 'system', timeout: 60
      });
      await task.update({ status: 'running', startedAt: new Date() });
      await fastify.task.services.runner();
      const updated = await fastify.task.services.detail({ id: task.id });
      expect(updated.status).to.equal('running');
    });
  });

  describe('waitingComplete 即时状态检查测试', () => {
    it('should reject when task is already canceled before call', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
      });
      await task.update({ status: 'success', output: { data: 'done' } });
      const result = await fastify.task.services.waitingComplete({ id: task.id, pollInterval: 10, maxPollTimes: 5 });
      expect(result).to.deep.equal({ data: 'done' });
    });
  });

  describe('waitingComplete 轮询路径测试', () => {
    it('should reject when task becomes failed during polling', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
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
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
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

  describe('TaskExecutor 路径验证与进度更新异常路径', () => {
    it('should skip invalid paths and continue searching other dirs', async () => {
      // Create a fastify instance with a dir containing a path-traversal type
      // that will fail validateScriptPath, but still find the task in another dir
      fastify = await createFastify({
        dirs: ['/nonexistent/path', path.resolve(__dirname, '../tasks')]
      }, taskData, taskIdCounter);
      await fastify.ready();
      // The executor should skip the invalid dir and find the task in the valid dir
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
      });
      await task.update({ status: 'running' });
      // Use executor directly
      const result = await fastify.task.services.executor({ type: 'test-type', task, log: () => {} });
      expect(result).to.exist;
    });

    it('should handle progress update failure gracefully', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'progress-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
      });
      await task.update({ status: 'running' });
      // Make task.update throw to trigger the catch in createProgressUpdater
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('DB error'); };
      // Run the task - progress-type calls updateProgress which should not crash
      await fastify.task.services.executor({ type: 'progress-type', task, log: () => {} });
      task.update = originalUpdate;
      // Task should still have completed without crashing
    });
  });

  describe('runner 任务执行失败日志路径', () => {
    it('should log error when processSystemTask throws in runner', async () => {
      fastify = await createFastify({}, taskData, taskIdCounter);
      await fastify.ready();
      const task = await fastify.task.services.create({
        type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'system'
      });
      // Corrupt task.update so processSystemTask throws at the beginning
      const originalUpdate = task.update;
      task.update = async () => { throw new Error('Critical DB error'); };
      await fastify.task.services.runner();
      // The error should have been logged, not thrown
      expect(fastify.log.error.called).to.be.true;
      task.update = originalUpdate;
    });
  });
});

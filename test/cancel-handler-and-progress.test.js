const { expect } = require('chai');
const sinon = require('sinon');
const crypto = require('node:crypto');
const { createTestContext } = require('./helpers/context');
const getTaskServiceContext = require('../libs/helpers/task-service-context');

describe('@kne/fastify-task - cancelHandler 与回调进度上报', function () {
  this.timeout(10000);

  const context = createTestContext();
  let fastify;

  const setup = async (taskConfig = {}, options = {}) => {
    fastify = await context.createFastify({
      ...options,
      task: { 'test-type': async () => {}, ...taskConfig }
    });
    await fastify.ready();
    return getTaskServiceContext(fastify, fastify.task.options);
  };

  const createTask = async (data = {}, status = 'running') => {
    const task = await fastify.task.services.create({ type: 'test-type', targetId: 'target-1', targetType: 'document', runnerType: 'manual', ...data });
    if (status !== 'pending') {
      await task.update({ status });
    }
    return task;
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

  describe('cancelHandler', () => {
    it('should call task cancelHandler after canceling a single task', async () => {
      const cancelHandler = sinon.stub().resolves();
      await setup({ 'test-type': { handler: async () => {}, cancelHandler } });
      const task = await createTask({ context: { runId: 7 } });

      await fastify.task.services.cancel({ id: task.id });

      expect(cancelHandler.calledOnce).to.be.true;
      const args = cancelHandler.firstCall.args[0];
      expect(args.task.id).to.equal(task.id);
      expect(args.task.status).to.equal('canceled');
      expect(args.context).to.deep.equal({ runId: 7 });
    });

    it('should call cancelHandler once per task on bulk cancel and skip finished tasks', async () => {
      const cancelHandler = sinon.stub().resolves();
      await setup({ 'test-type': { handler: async () => {}, cancelHandler } });
      const running = await createTask();
      const pending = await createTask({}, 'pending');
      const done = await createTask({}, 'success');

      const affected = await fastify.task.services.cancel({ targetId: 'target-1', targetType: 'document', type: 'test-type' });

      expect(affected).to.equal(2);
      expect(cancelHandler.callCount).to.equal(2);
      expect(cancelHandler.args.map(([arg]) => arg.task.id).sort()).to.deep.equal([running.id, pending.id].sort());
      expect((await fastify.task.services.detail({ id: done.id })).status).to.equal('success');
    });

    it('should not call cancelHandler when the task is already finished', async () => {
      const cancelHandler = sinon.stub().resolves();
      await setup({ 'test-type': { handler: async () => {}, cancelHandler } });
      const task = await createTask({}, 'success');

      await fastify.task.services.cancel({ id: task.id });

      expect(cancelHandler.called).to.be.false;
      expect((await fastify.task.services.detail({ id: task.id })).status).to.equal('success');
    });

    it('should prefer task cancelHandler over the global one', async () => {
      const taskCancelHandler = sinon.stub().resolves();
      const globalCancelHandler = sinon.stub().resolves();
      await setup({ 'test-type': { handler: async () => {}, cancelHandler: taskCancelHandler } }, { cancelHandler: globalCancelHandler });
      const task = await createTask();

      await fastify.task.services.cancel({ id: task.id });

      expect(taskCancelHandler.calledOnce).to.be.true;
      expect(globalCancelHandler.called).to.be.false;
    });

    it('should fallback to the global cancelHandler when the task has none', async () => {
      const globalCancelHandler = sinon.stub().resolves();
      await setup({}, { cancelHandler: globalCancelHandler });
      const task = await createTask();

      await fastify.task.services.cancel({ id: task.id });

      expect(globalCancelHandler.calledOnce).to.be.true;
      expect(globalCancelHandler.firstCall.args[0].task.id).to.equal(task.id);
    });

    it('should keep the task canceled when cancelHandler throws', async () => {
      const cancelHandler = sinon.stub().rejects(new Error('cancel boom'));
      await setup({ 'test-type': { handler: async () => {}, cancelHandler } });
      const task = await createTask();

      await fastify.task.services.cancel({ id: task.id });

      expect(cancelHandler.calledOnce).to.be.true;
      expect((await fastify.task.services.detail({ id: task.id })).status).to.equal('canceled');
    });

    it('should reject non-function cancelHandler in task declaration', async () => {
      const ctx = await setup();
      expect(() => ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'bad-cancel', config: { cancelHandler: 1 } })).to.throw('任务 bad-cancel 的 cancelHandler 必须是一个函数');
    });
  });

  describe('callback 进度上报', () => {
    it('should only update progress and log when code is 0 and progress is below 100', async () => {
      const handler = sinon.stub().resolves();
      await setup({ 'test-type': { handler } });
      const task = await createTask();

      await fastify.task.services.callback({ id: task.id, code: 0, data: { step: 'parse' }, message: '解析中', progress: 40 });

      const current = await fastify.task.services.detail({ id: task.id });
      expect(current.status).to.equal('running');
      expect(current.progress).to.equal(40);
      expect(current.completedAt || null).to.equal(null);
      expect(handler.called).to.be.false;
      expect(current.options.logs).to.have.length(1);
      expect(current.options.logs[0].message).to.equal('解析中');
      expect(current.options.logs[0].data).to.deep.equal({ code: 0, data: { step: 'parse' }, message: '解析中', progress: 40 });
    });

    it('should finish the task with the final callback after progress reports', async () => {
      const handler = sinon.stub().resolves();
      await setup({ 'test-type': { handler } });
      const task = await createTask();

      await fastify.task.services.callback({ id: task.id, code: 0, progress: 30 });
      await fastify.task.services.callback({ id: task.id, code: 0, data: { ok: true }, message: 'done' });

      const current = await fastify.task.services.detail({ id: task.id });
      expect(current.status).to.equal('success');
      expect(current.progress).to.equal(100);
      expect(current.output).to.deep.equal({ ok: true });
      expect(handler.calledOnce).to.be.true;
      expect(current.options.logs[0].data).to.deep.equal({ code: 0, data: { ok: true }, message: 'done' });
    });

    it('should treat progress 100 as the final result', async () => {
      await setup();
      const task = await createTask();

      await fastify.task.services.callback({ id: task.id, code: 0, data: { ok: true }, progress: 100 });

      expect((await fastify.task.services.detail({ id: task.id })).status).to.equal('success');
    });

    it('should fail the task when code is not 0 even with progress', async () => {
      await setup();
      const task = await createTask();

      await fastify.task.services.callback({ id: task.id, code: 500, message: 'upstream error', progress: 50 });

      const current = await fastify.task.services.detail({ id: task.id });
      expect(current.status).to.equal('failed');
      expect(current.error).to.include('upstream error');
    });

    it('should ignore progress reports for tasks that are no longer active', async () => {
      await setup();
      const task = await createTask();
      await fastify.task.services.cancel({ id: task.id });

      await fastify.task.services.callback({ id: task.id, code: 0, progress: 60 });

      const current = await fastify.task.services.detail({ id: task.id });
      expect(current.status).to.equal('canceled');
      expect(current.progress).to.equal(0);
      expect(current.options.logs || []).to.have.length(0);
    });

    it('should round fractional progress to an integer', async () => {
      await setup();
      const task = await createTask();

      await fastify.task.services.callback({ id: task.id, code: 0, progress: 33.6 });

      expect((await fastify.task.services.detail({ id: task.id })).progress).to.equal(34);
    });

    it('should reject progress outside 0-100', async () => {
      await setup();
      const task = await createTask();

      let error;
      try {
        await fastify.task.services.callback({ id: task.id, code: 0, progress: 101 });
      } catch (e) {
        error = e;
      }
      expect(error.message).to.equal('progress 必须是 0-100 之间的数字');
      expect((await fastify.task.services.detail({ id: task.id })).status).to.equal('running');
    });

    it('POST /callback should accept progress through the same endpoint', async () => {
      await setup();
      const task = await createTask();

      const progressResponse = await fastify.inject({ method: 'POST', url: `/api/task/callback?taskId=${task.id}`, payload: { code: 0, progress: 20, message: '排队中' } });
      expect(progressResponse.statusCode).to.equal(200);
      expect(await fastify.task.services.detail({ id: task.id })).to.include({ status: 'running', progress: 20 });

      const finalResponse = await fastify.inject({ method: 'POST', url: `/api/task/callback?taskId=${task.id}`, payload: { code: 0, data: { ok: true } } });
      expect(finalResponse.statusCode).to.equal(200);
      expect((await fastify.task.services.detail({ id: task.id })).status).to.equal('success');
    });

    it('POST /callback should reject progress out of range by schema', async () => {
      await setup();
      const task = await createTask();

      const response = await fastify.inject({ method: 'POST', url: `/api/task/callback?taskId=${task.id}`, payload: { code: 0, progress: -1 } });

      expect(response.statusCode).to.equal(400);
    });

    it('POST /callback should reject a body without code and keep the task running', async () => {
      await setup();
      const task = await createTask();

      const response = await fastify.inject({ method: 'POST', url: `/api/task/callback?taskId=${task.id}`, payload: { data: { ok: true } } });

      expect(response.statusCode).to.equal(400);
      expect((await fastify.task.services.detail({ id: task.id })).status).to.equal('running');
    });

    describe('签名', () => {
      const sign = ({ secret, id, payload }) =>
        crypto
          .createHmac('sha256', secret)
          .update(`${id}|${JSON.stringify(payload)}`)
          .digest('hex');

      it('should verify signature over { code, data, message, progress } for progress reports', async () => {
        await setup();
        const secret = 'progress-secret';
        const task = await createTask({}, 'running');
        await task.update({ context: { secret } });
        const body = { code: 0, data: { step: 1 }, message: '处理中', progress: 55 };

        await fastify.task.services.callbackWithSignature({ id: task.id, ...body, signature: sign({ secret, id: task.id, payload: body }) });

        expect(await fastify.task.services.detail({ id: task.id })).to.include({ status: 'running', progress: 55 });
      });

      it('should reject a progress report whose signature does not cover progress', async () => {
        await setup();
        const secret = 'progress-secret';
        const task = await createTask({}, 'running');
        await task.update({ context: { secret } });
        const body = { code: 0, data: { step: 1 }, message: '处理中' };

        let error;
        try {
          await fastify.task.services.callbackWithSignature({ id: task.id, ...body, progress: 55, signature: sign({ secret, id: task.id, payload: body }) });
        } catch (e) {
          error = e;
        }

        expect(error.message).to.equal('签名验证失败');
        expect((await fastify.task.services.detail({ id: task.id })).progress).to.equal(0);
      });
    });
  });
});

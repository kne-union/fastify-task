const { expect } = require('chai');
const sinon = require('sinon');
const { createTestContext } = require('./helpers/context');
const getTaskServiceContext = require('../libs/helpers/task-service-context');

describe('@kne/fastify-task - 任务链、命名空间与错误处理', function () {
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

  const createRunningTask = async (ctx, data) => {
    const task = await ctx.create(data);
    task.status = 'running';
    return task;
  };

  const findChildren = async parentTaskId => fastify.task.models.task.findAll({ where: { parentTaskId } });

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

  describe('任务链 next 测试', () => {
    it('should create next task with output as input and cloned context when next is a string', async () => {
      const handler = sinon.stub().resolves();
      const ctx = await setup({ 'chain-a': { handler, next: 'chain-b' }, 'chain-b': async () => {} });
      const parentContext = { step: 1 };
      const task = await createRunningTask(ctx, { type: 'chain-a', input: { name: '候选人A' }, context: parentContext, runnerType: 'system', userId: 9 });

      const result = await ctx.finalizeSuccess({ task, output: { score: 90 } });

      expect(result).to.be.true;
      expect(handler.calledOnce).to.be.true;
      expect(handler.firstCall.args[0].result).to.deep.equal({ score: 90 });
      const [child] = await findChildren(task.id);
      expect(child.type).to.equal('chain-b');
      expect(child.targetId).to.equal(task.id);
      expect(child.targetType).to.equal('task');
      expect(child.runnerType).to.equal('system');
      expect(child.userId).to.equal(9);
      expect(child.input).to.deep.equal({ score: 90, name: '候选人A' });
      expect(child.context).to.deep.equal({ step: 1 });
      expect(child.context).to.not.equal(parentContext);
    });

    it('should wrap non-object output into data when creating next task', async () => {
      const ctx = await setup({ 'chain-a': { next: 'chain-b' }, 'chain-b': async () => {} });
      const task = await createRunningTask(ctx, { type: 'chain-a' });

      await ctx.finalizeSuccess({ task, output: 'plain-text' });

      const [child] = await findChildren(task.id);
      expect(child.input).to.deep.equal({ data: 'plain-text' });
      expect(child.context).to.deep.equal({});
    });

    it('should create the next task chosen by output.next when next is an allow list', async () => {
      const ctx = await setup({ 'chain-a': { next: ['chain-b', 'chain-c'] }, 'chain-b': async () => {}, 'chain-c': async () => {} });
      const task = await createRunningTask(ctx, { type: 'chain-a' });

      await ctx.finalizeSuccess({ task, output: { next: 'chain-c' } });

      const children = await findChildren(task.id);
      expect(children.map(item => item.type)).to.deep.equal(['chain-c']);
    });

    it('should not create next task when next is not configured', async () => {
      const ctx = await setup({ 'chain-a': { handler: async () => {} } });
      const task = await createRunningTask(ctx, { type: 'chain-a' });

      await ctx.finalizeSuccess({ task, output: { score: 1 } });

      expect(await findChildren(task.id)).to.have.length(0);
      expect(task.status).to.equal('success');
    });

    const failCases = [
      { title: 'output.next is missing for allow list', config: { next: ['chain-b'] }, output: {}, message: '未指定下一个任务' },
      { title: 'output.next is outside allow list', config: { next: ['chain-b'] }, output: { next: 'chain-x' }, message: '不在允许列表中' },
      { title: 'next config is neither string nor array', config: { next: 1 }, output: {}, message: 'next 配置必须是字符串或数组' },
      { title: 'next task declaration does not exist', config: { next: 'missing-type' }, output: {}, message: '未找到下一个任务声明:missing-type' }
    ];

    failCases.forEach(({ title, config, output, message }) => {
      it(`should fail the task and throw when ${title}`, async () => {
        const errorHandler = sinon.stub().resolves();
        const ctx = await setup({ 'chain-a': config, 'chain-b': async () => {} }, { errorHandler });
        const task = await createRunningTask(ctx, { type: 'chain-a' });

        let caught;
        try {
          await ctx.finalizeSuccess({ task, output });
        } catch (e) {
          caught = e;
        }

        expect(caught).to.be.instanceOf(Error);
        expect(caught.message).to.include(message);
        expect(task.status).to.equal('failed');
        expect(task.error).to.include(message);
        expect(errorHandler.calledOnce).to.be.true;
        expect(await findChildren(task.id)).to.have.length(0);
      });
    });
  });

  describe('命名空间隔离测试', () => {
    it('should resolve next task inside the same namespace when type has no prefix', async () => {
      const ctx = await setup();
      ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'sub-a', config: { next: 'sub-b' } });
      ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'sub-b', config: async () => {} });
      const task = await createRunningTask(ctx, { type: 'plugin.sub-a' });

      await ctx.finalizeSuccess({ task, output: {} });

      const [child] = await findChildren(task.id);
      expect(task.type).to.equal('plugin.sub-a');
      expect(child.type).to.equal('plugin.sub-b');
    });

    it('should reject a plugin task chaining into the main project namespace', async () => {
      const ctx = await setup({ 'chain-b': async () => {} });
      ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'sub-a', config: { next: 'task.chain-b' } });
      const task = await createRunningTask(ctx, { type: 'plugin.sub-a' });

      let caught;
      try {
        await ctx.finalizeSuccess({ task, output: {} });
      } catch (e) {
        caught = e;
      }

      expect(caught.message).to.equal('任务 plugin.sub-a 不允许访问主项目任务:task.chain-b');
      expect(task.status).to.equal('failed');
      expect(await findChildren(task.id)).to.have.length(0);
    });

    it('should allow the main project task chaining into a plugin namespace', async () => {
      const ctx = await setup({ 'chain-a': { next: 'plugin.sub-b' } });
      ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'sub-b', config: async () => {} });
      const task = await createRunningTask(ctx, { type: 'chain-a' });

      await ctx.finalizeSuccess({ task, output: {} });

      const [child] = await findChildren(task.id);
      expect(child.type).to.equal('plugin.sub-b');
    });

    it('should reject non-function handler or errorHandler in task declaration', async () => {
      const ctx = await setup();

      expect(() => ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'bad-a', config: { handler: 'x' } })).to.throw('任务 bad-a 的 handler 必须是一个函数');
      expect(() => ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'bad-b', config: { errorHandler: 1 } })).to.throw('任务 bad-b 的 errorHandler 必须是一个函数');
    });
  });

  describe('错误处理回退测试', () => {
    it('should only call task errorHandler when it handles the error', async () => {
      const taskErrorHandler = sinon.stub().resolves();
      const globalErrorHandler = sinon.stub().resolves();
      const ctx = await setup({ 'err-type': { errorHandler: taskErrorHandler } }, { errorHandler: globalErrorHandler });
      const task = await createRunningTask(ctx, { type: 'err-type', context: { from: 'test' } });
      const error = new Error('boom');

      const result = await ctx.failTask({ task, error, collect: false });

      expect(result).to.be.true;
      expect(task.status).to.equal('failed');
      expect(taskErrorHandler.calledOnce).to.be.true;
      expect(taskErrorHandler.firstCall.args[0]).to.include({ task, error });
      expect(taskErrorHandler.firstCall.args[0].context).to.deep.equal({ from: 'test' });
      expect(globalErrorHandler.called).to.be.false;
    });

    it('should fallback to global errorHandler with the error thrown by task errorHandler', async () => {
      const handlerError = new Error('handler failed');
      const globalErrorHandler = sinon.stub().resolves();
      const ctx = await setup({ 'err-type': { errorHandler: sinon.stub().rejects(handlerError) } }, { errorHandler: globalErrorHandler });
      const task = await createRunningTask(ctx, { type: 'err-type' });

      await ctx.failTask({ task, error: new Error('boom'), collect: false });

      expect(globalErrorHandler.calledOnce).to.be.true;
      expect(globalErrorHandler.firstCall.args[0].error).to.equal(handlerError);
    });

    it('should log and swallow the error when global errorHandler throws', async () => {
      const globalError = new Error('global failed');
      const ctx = await setup({ 'err-type': async () => {} }, { errorHandler: sinon.stub().rejects(globalError) });
      const task = await createRunningTask(ctx, { type: 'err-type' });

      const result = await ctx.failTask({ task, error: new Error('boom'), collect: false });

      expect(result).to.be.true;
      expect(task.status).to.equal('failed');
      expect(fastify.log.error.calledWith(globalError)).to.be.true;
    });

    it('should use errorHandler from a namespaced task declaration', async () => {
      const taskErrorHandler = sinon.stub().resolves();
      const globalErrorHandler = sinon.stub().resolves();
      const ctx = await setup({}, { errorHandler: globalErrorHandler });
      ctx.registerTaskDeclaration({ namespace: 'plugin', taskName: 'err-sub', config: { errorHandler: taskErrorHandler } });
      const task = await createRunningTask(ctx, { type: 'plugin.err-sub' });

      await ctx.failTask({ task, error: new Error('boom'), collect: false });

      expect(taskErrorHandler.calledOnce).to.be.true;
      expect(globalErrorHandler.called).to.be.false;
    });

    it('should not handle error again when task already finished', async () => {
      const globalErrorHandler = sinon.stub().resolves();
      const ctx = await setup({ 'err-type': async () => {} }, { errorHandler: globalErrorHandler });
      const task = await ctx.create({ type: 'err-type' });
      task.status = 'canceled';

      const result = await ctx.failTask({ task, error: new Error('boom'), collect: false });

      expect(result).to.be.false;
      expect(task.status).to.equal('canceled');
      expect(globalErrorHandler.called).to.be.false;
    });
  });

  describe('任务已结束保护测试', () => {
    const runCanceledTask = async mode => {
      const ctx = await setup({ 'guard-type': async () => {} });
      const task = await ctx.create({ type: 'guard-type', input: { mode }, context: { origin: true } });
      task.status = 'canceled';
      const run = ctx.executor({ type: 'guard-type', task });
      return { task, run };
    };

    it('should skip updateProgress when task already finished', async () => {
      const { task, run } = await runCanceledTask('progress');

      expect(await run).to.deep.equal({ result: 'success' });
      expect(task.progress).to.not.equal(80);
    });

    it('should return false and keep status when next is called on finished task', async () => {
      const { task, run } = await runCanceledTask('next');

      expect(await run).to.deep.equal({ nextResult: false });
      expect(task.status).to.equal('canceled');
      expect(task.context).to.deep.equal({ origin: true });
    });

    it('should reject polling when task already finished', async () => {
      const { task, run } = await runCanceledTask('polling');

      let caught;
      try {
        await run;
      } catch (e) {
        caught = e;
      }

      expect(caught.message).to.equal('任务已结束:canceled');
      expect(task.pollCount).to.equal(0);
    });

    it('should switch task to waiting with new context when next is called on running task', async () => {
      const ctx = await setup({ 'guard-type': async () => {} });
      const task = await createRunningTask(ctx, { type: 'guard-type', input: { mode: 'next' } });

      expect(await ctx.executor({ type: 'guard-type', task })).to.deep.equal({ nextResult: false });
      expect(task.status).to.equal('waiting');
      expect(task.context).to.deep.equal({ changed: true });
    });
  });
});

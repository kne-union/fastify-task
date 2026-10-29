const { expect } = require('chai');
const sinon = require('sinon');
const { createTestContext } = require('./helpers/context');

describe('@kne/fastify-task - 搜索、调度并发与异常分支', function () {
  this.timeout(10000);

  const context = createTestContext();
  let fastify;

  const createFastify = async options => {
    fastify = await context.createFastify(options);
    await fastify.ready();
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

  describe('列表 JSON 字段搜索测试', () => {
    const searchWithDialect = async (dialect, prepare) => {
      await createFastify();
      const { Sequelize } = fastify.sequelize;
      Sequelize.Op.and = Symbol('and');
      Sequelize.where = (left, condition) => ({ left, condition });
      if (prepare) prepare(Sequelize);
      fastify.task.models.task.sequelize.getDialect.returns(dialect);
      const findSpy = sinon.spy(fastify.task.models.task, 'findAndCountAll');
      await fastify.task.services.list({ filter: { targetName: 'Caribbean' } });
      return { Op: Sequelize.Op, where: findSpy.firstCall.args[0].where };
    };

    it('should search input.name case-insensitively with ->> on postgres', async () => {
      const { Op, where } = await searchWithDialect('postgres');

      expect(where[Op.and]).to.have.length(1);
      expect(where[Op.and][0]).to.deep.equal({
        left: { fn: 'lower', args: [{ literal: `"input"->>'name'` }] },
        condition: { [Op.like]: '%caribbean%' }
      });
      expect(where).to.not.have.property('input.name');
    });

    it('should search input.name with json_extract on sqlite', async () => {
      const { Op, where } = await searchWithDialect('sqlite');

      expect(where[Op.and][0].left).to.deep.equal({ fn: 'lower', args: [{ literal: `json_extract("input", '$.name')` }] });
    });

    it('should use Sequelize.json for other dialects', async () => {
      const { Op, where } = await searchWithDialect('mysql', Sequelize => {
        Sequelize.json = path => ({ json: path });
      });

      expect(where[Op.and][0].left).to.deep.equal({ fn: 'lower', args: [{ json: 'input.name' }] });
    });

    it('should fallback to plain like on input.name when json expression is unavailable', async () => {
      const { Op, where } = await searchWithDialect('mysql');

      expect(where[Op.and]).to.be.undefined;
      expect(where['input.name']).to.deep.equal({ [Op.like]: '%Caribbean%' });
    });

    it('should fallback to plain like when Sequelize.literal is unavailable', async () => {
      const { Op, where } = await searchWithDialect('postgres', Sequelize => {
        delete Sequelize.literal;
      });

      expect(where[Op.and]).to.be.undefined;
      expect(where['input.name']).to.deep.equal({ [Op.like]: '%Caribbean%' });
    });
  });

  describe('列表嵌套筛选参数测试', () => {
    it('should parse filter[createdAt][startTime] bracket query into nested time filter', async () => {
      await createFastify();
      const oldTask = await fastify.task.services.create({ type: 'test-type', targetId: 'old', targetType: 'document' });
      oldTask.createdAt = new Date('2026-01-01T00:00:00.000Z');
      await fastify.task.services.create({ type: 'test-type', targetId: 'new', targetType: 'document' });

      const response = await fastify.inject({
        method: 'GET',
        url: `/api/task/list?filter%5BcreatedAt%5D%5BstartTime%5D=${encodeURIComponent('2026-06-01T00:00:00.000Z')}`
      });
      const result = JSON.parse(response.payload);

      expect(response.statusCode).to.equal(200);
      expect(result.totalCount).to.equal(1);
      expect(result.pageData[0].targetId).to.equal('new');
    });

    it('should replace non-object filter value when nested bracket key is present', async () => {
      await createFastify();
      const findSpy = sinon.spy(fastify.task.models.task, 'findAndCountAll');

      await fastify.task.services.list({ filter: 'invalid', 'filter[completedAt][endTime]': '2026-06-01T00:00:00.000Z' });

      const { where } = findSpy.firstCall.args[0];
      const { Op } = fastify.sequelize.Sequelize;
      expect(where.completedAt).to.deep.equal({ [Op.lte]: new Date('2026-06-01T00:00:00.000Z') });
    });
  });

  describe('多实例抢占任务测试', () => {
    const createSystemTask = targetId => fastify.task.services.create({ type: 'test-type', targetId, targetType: 'document', runnerType: 'system' });

    it('should return null when task was already claimed by another instance', async () => {
      await createFastify();
      const task = await createSystemTask('t1');
      task.status = 'running';

      expect(await fastify.task.services.claimTask(task)).to.be.null;
    });

    it('should not execute task when processSystemTask fails to claim it', async () => {
      await createFastify();
      const task = await createSystemTask('t1');
      task.status = 'running';
      const executorSpy = sinon.spy(require('../libs/helpers/task-service-context')(fastify, fastify.task.options), 'executor');

      expect(await fastify.task.services.processSystemTask(task)).to.be.false;
      expect(executorSpy.called).to.be.false;
    });

    it('should return empty list when no capacity is left', async () => {
      await createFastify();
      await createSystemTask('t1');

      expect(await fastify.task.services.claimPendingTasks(0)).to.deep.equal([]);
    });

    it('should skip tasks claimed concurrently or removed after claim', async () => {
      await createFastify();
      const taken = await createSystemTask('taken');
      const removed = await createSystemTask('removed');
      const normal = await createSystemTask('normal');
      const model = fastify.task.models.task;
      const originalUpdate = model.update;
      const originalFindByPk = model.findByPk;
      sinon.stub(model, 'update').callsFake(async (data, opts) => (opts.where.id === taken.id ? [0] : originalUpdate(data, opts)));
      sinon.stub(model, 'findByPk').callsFake(async id => (id === removed.id ? null : originalFindByPk(id)));

      const claimed = await fastify.task.services.claimPendingTasks(10);

      expect(claimed.map(item => item.targetId)).to.deep.equal(['normal']);
      expect(normal.status).to.equal('running');
      expect(taken.status).to.equal('pending');
    });
  });

  describe('任务超时定时器测试', () => {
    it('should clear timeout timer after task finishes', async () => {
      await createFastify({ taskTimeout: 123456 });
      const task = await fastify.task.services.create({ type: 'test-type', targetId: 't1', targetType: 'document', runnerType: 'system' });
      const setTimeoutSpy = sinon.spy(global, 'setTimeout');
      const clearTimeoutSpy = sinon.spy(global, 'clearTimeout');

      try {
        await fastify.task.services.processSystemTask(task);
      } finally {
        setTimeoutSpy.restore();
        clearTimeoutSpy.restore();
      }

      const timeoutCall = setTimeoutSpy.getCalls().find(call => call.args[1] === 123456);
      expect(task.status).to.equal('success');
      expect(timeoutCall).to.exist;
      expect(clearTimeoutSpy.calledWith(timeoutCall.returnValue)).to.be.true;
    });
  });

  describe('失败重试分支测试', () => {
    const runGuardTask = async (mode, maxRetries) => {
      await createFastify({ task: { 'guard-type': async () => {} }, retryBaseDelay: 1000 });
      const task = await fastify.task.services.create({ type: 'guard-type', runnerType: 'system', input: { mode }, maxRetries });
      await fastify.task.services.processSystemTask(task);
      return task;
    };

    it('should not schedule retry when task was canceled during execution', async () => {
      const task = await runGuardTask('cancel-then-throw', 2);

      expect(task.status).to.equal('canceled');
      expect(task.retryCount).to.equal(0);
      expect(fastify.log.info.calledWithMatch(/重试/)).to.be.false;
    });

    it('should schedule retry with empty error when thrown value has no stack', async () => {
      const task = await runGuardTask('throw-without-stack', 1);

      expect(task.status).to.equal('pending');
      expect(task.retryCount).to.equal(1);
      expect(task.error).to.equal('');
      expect(task.startTime.getTime()).to.be.greaterThan(Date.now());
      expect(fastify.log.info.calledWithMatch(/第 1 次重试，1000ms 后执行/)).to.be.true;
    });
  });

  describe('回调 next 结果处理测试', () => {
    it('should fail task and rethrow when finalizing next result throws', async () => {
      const errorHandler = sinon.stub().resolves();
      await createFastify({ task: { 'chain-a': { next: 'missing-type' } }, errorHandler });
      const task = await fastify.task.services.create({ type: 'chain-a', targetId: 't1', targetType: 'document' });
      task.status = 'waiting';
      const result = JSON.stringify({ code: 0, data: { ok: true } });

      let caught;
      try {
        await fastify.task.services.processNext({ id: task.id, result });
      } catch (e) {
        caught = e;
      }

      expect(caught.message).to.equal('未找到下一个任务声明:missing-type');
      expect(task.status).to.equal('failed');
      expect(task.output).to.deep.equal({ code: 0, data: { ok: true } });
      expect(errorHandler.calledOnce).to.be.true;
    });
  });

  describe('统计按执行方式筛选测试', () => {
    it('should summarize only matching runner type channels when filtering by runnerType', async () => {
      await createFastify();
      const now = new Date();
      const list = [
        { channel: 'test-type', time: now, data: { sum: { total: 10, success: 8, failed: 1, canceled: 1 } } },
        { channel: 'test-type:manual', time: now, data: { sum: { total: 5, success: 4, failed: 1, canceled: 0 } } },
        { channel: 'test-type:system', time: now, data: { sum: { total: 5, success: 4, failed: 0, canceled: 1 } } }
      ];
      fastify.taskStatistics.services.query.callsFake(async params => {
        const start = new Date(params.startTime).getTime();
        const end = new Date(params.endTime).getTime();
        return { channelMetas: {}, list: list.filter(item => item.time.getTime() >= start && item.time.getTime() <= end) };
      });

      const result = await fastify.task.services.queryStatistics({ range: '7d', runnerType: 'manual' });

      expect(result.totalTasks).to.equal(5);
      expect(result.byStatus).to.deep.equal({ success: 4, failed: 1, canceled: 0 });
      expect(result.byType).to.deep.equal({ 'test-type': 5 });
      expect(result.byRunnerType).to.deep.equal({ manual: 5 });
      expect(result.duration.completedCount).to.equal(5);
      expect(Object.keys(result.duration.byRunnerType)).to.deep.equal(['manual']);
    });
  });
});

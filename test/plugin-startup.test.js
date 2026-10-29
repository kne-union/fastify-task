const { expect } = require('chai');
const sinon = require('sinon');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Fastify = require('fastify');
const fp = require('fastify-plugin');

const TASK_CRON_TIME = '*/7 * * * *';

const createApp = async ({ storage = ':memory:', taskOptions = {} } = {}) => {
  const fastify = Fastify({ logger: false });
  const createJob = sinon.stub();
  await fastify.register(require('@kne/fastify-sequelize'), { db: { dialect: 'sqlite', storage, logging: false } });
  await fastify.register(
    fp(
      async instance => {
        instance.decorate('cron', { createJob });
      },
      { name: 'fastify-cron' }
    )
  );
  let userModel;
  await fastify.register(
    fp(async instance => {
      userModel = instance.sequelize.instance.define('startupUser', {});
    })
  );
  await fastify.register(
    require('../index'),
    Object.assign(
      {
        cronTime: null,
        dir: path.resolve(__dirname, './tasks'),
        getUserModel: () => userModel,
        getAuthenticate: () => []
      },
      taskOptions
    )
  );
  const getTaskJobs = () =>
    createJob
      .getCalls()
      .map(call => call.args[0])
      .filter(job => job.cronTime === (taskOptions.cronTime || TASK_CRON_TIME));
  return { fastify, getTaskJobs };
};

describe('@kne/fastify-task - 插件启动与真实数据库', function () {
  this.timeout(10000);

  describe('定时任务注册测试', () => {
    it('should not create cron job when cronTime is empty', async () => {
      const { fastify, getTaskJobs } = await createApp();
      await fastify.ready();
      await fastify.close();

      expect(getTaskJobs()).to.have.length(0);
    });

    it('should create cron job whose tick runs pending system tasks', async () => {
      const { fastify, getTaskJobs } = await createApp({ taskOptions: { cronTime: TASK_CRON_TIME, recoverRunningTasksOnStart: false } });
      await fastify.ready();
      await fastify.sequelize.instance.sync();

      const jobs = getTaskJobs();
      expect(jobs).to.have.length(1);
      const { start, onTick } = jobs[0];
      expect(start).to.be.true;

      const task = await fastify.task.services.create({ type: 'test-type', targetId: '1', targetType: 'document', runnerType: 'system' });
      await onTick();
      await task.reload();
      await fastify.close();

      expect(task.status).to.equal('success');
    });
  });

  describe('启动恢复陈旧任务测试', () => {
    let storage;

    beforeEach(async () => {
      storage = path.join(os.tmpdir(), `fastify-task-startup-${process.pid}-${Date.now()}.sqlite`);
      const { fastify } = await createApp({ storage });
      await fastify.ready();
      await fastify.sequelize.instance.sync();
      const { task } = fastify.task.models;
      const base = { type: 'test-type', scriptName: 'index', targetId: '1', targetType: 'document', runnerType: 'system', status: 'running' };
      await task.create(Object.assign({}, base, { input: { name: 'stale' }, startedAt: new Date(Date.now() - 2 * 60 * 60 * 1000) }));
      await task.create(Object.assign({}, base, { input: { name: 'fresh' }, startedAt: new Date() }));
      await fastify.close();
    });

    afterEach(() => {
      fs.rmSync(storage, { force: true });
    });

    const getStatusByName = async fastify => {
      const tasks = await fastify.task.models.task.findAll();
      return Object.fromEntries(tasks.map(item => [item.input.name, item.status]));
    };

    it('should reset only stale running tasks on ready', async () => {
      const { fastify } = await createApp({ storage, taskOptions: { cronTime: '*/1 * * * *', taskTimeout: 60 * 60 * 1000 } });
      await fastify.ready();
      const statusByName = await getStatusByName(fastify);
      await fastify.close();

      expect(statusByName).to.deep.equal({ stale: 'pending', fresh: 'running' });
    });

    it('should keep running tasks when recoverRunningTasksOnStart is false', async () => {
      const { fastify } = await createApp({ storage, taskOptions: { cronTime: '*/1 * * * *', recoverRunningTasksOnStart: false } });
      await fastify.ready();
      const statusByName = await getStatusByName(fastify);
      await fastify.close();

      expect(statusByName).to.deep.equal({ stale: 'running', fresh: 'running' });
    });
  });

  describe('真实 sqlite 目标名称搜索测试', () => {
    let fastify;

    beforeEach(async () => {
      ({ fastify } = await createApp());
      await fastify.ready();
      await fastify.sequelize.instance.sync();
      const { create } = fastify.task.services;
      await create({ type: 'test-type', targetId: '1', targetType: 'document', input: { name: 'Caribbean Cruise' } });
      await create({ type: 'test-type', targetId: '2', targetType: 'document', input: { name: 'Alpine Trip' } });
      await create({ type: 'test-type', targetId: '3', targetType: 'document' });
    });

    afterEach(async () => {
      await fastify.close();
    });

    it('should match input.name case-insensitively through services.list', async () => {
      const result = await fastify.task.services.list({ filter: { targetName: 'cARIB' } });

      expect(result.totalCount).to.equal(1);
      expect(result.pageData[0].input.name).to.equal('Caribbean Cruise');
    });

    it('should match input.name through bracket query on the list route', async () => {
      const response = await fastify.inject({ method: 'GET', url: `/api/task/list?filter[targetName]=${encodeURIComponent('TRIP')}` });
      const body = JSON.parse(response.body);
      const data = body.data || body;

      expect(response.statusCode).to.equal(200);
      expect(data.totalCount).to.equal(1);
      expect(data.pageData[0].input.name).to.equal('Alpine Trip');
    });
  });
});

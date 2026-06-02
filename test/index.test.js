const { expect } = require('chai');
const sinon = require('sinon');
const { createFastify } = require('./helpers/setup');

describe('@kne/fastify-task - 插件注册', function () {
  this.timeout(10000);

  let fastify;
  let taskData = [];
  let taskIdCounter = { value: 1 };

  beforeEach(() => {
    taskData = [];
    taskIdCounter.value = 1;
  });

  afterEach(async () => {
    if (fastify) {
      await fastify.close();
      fastify = null;
    }
    sinon.restore();
  });

  it('should register plugin with default options', async () => {
    fastify = await createFastify({}, taskData, taskIdCounter);
    await fastify.ready();
    expect(fastify.task).to.exist;
    expect(fastify.task.services).to.exist;
    expect(fastify.task.models).to.exist;
  });

  it('should register plugin with custom options', async () => {
    fastify = await createFastify({ name: 'task', limit: 5 }, taskData, taskIdCounter);
    await fastify.ready();
    expect(fastify.task).to.exist;
    expect(fastify.task.options.limit).to.equal(5);
  });

  it('should expose all required services', async () => {
    fastify = await createFastify({}, taskData, taskIdCounter);
    await fastify.ready();
    const services = fastify.task.services;
    expect(services.create).to.exist;
    expect(services.detail).to.exist;
    expect(services.list).to.exist;
    expect(services.complete).to.exist;
    expect(services.cancel).to.exist;
    expect(services.runner).to.exist;
    expect(services.resetAll).to.exist;
    expect(services.retry).to.exist;
    expect(services.log).to.exist;
    expect(services.callback).to.exist;
  });

  it('should register all API routes', async () => {
    fastify = await createFastify({}, taskData, taskIdCounter);
    await fastify.ready();
    const routes = [
      { method: 'GET', url: '/api/task/list' },
      { method: 'POST', url: '/api/task/complete' },
      { method: 'POST', url: '/api/task/cancel' },
      { method: 'POST', url: '/api/task/retry' },
      { method: 'POST', url: '/api/task/next' },
      { method: 'POST', url: '/api/task/log' },
      { method: 'POST', url: '/api/task/callback' },
      { method: 'GET', url: '/api/task/statistics' },
      { method: 'GET', url: '/api/task/statistics/sse' }
    ];
    for (const route of routes) {
      const response = await fastify.inject(route);
      expect(response.statusCode, `${route.method} ${route.url}`).to.not.equal(404);
    }
  });
});

const { expect } = require('chai');
const path = require('node:path');
const Fastify = require('fastify');
const fp = require('fastify-plugin');

describe('@kne/fastify-task - OpenAPI 文档完整性', function () {
  this.timeout(10000);

  it('所有路由都有 tags、summary，参数都有用途说明，并声明 200 响应', async () => {
    const fastify = Fastify({ logger: false });
    const routes = [];
    fastify.addHook('onRoute', route => route.method !== 'HEAD' && routes.push(route));
    await fastify.register(require('@kne/fastify-sequelize'), { db: { dialect: 'sqlite', storage: ':memory:', logging: false } });
    await fastify.register(require('fastify-cron'));
    let userModel;
    await fastify.register(
      fp(async instance => {
        userModel = instance.sequelize.instance.define('docUser', {});
      })
    );
    await fastify.register(require('../index'), {
      cronTime: null,
      dir: path.resolve(__dirname, './tasks'),
      getUserModel: () => userModel,
      getAuthenticate: () => []
    });
    await fastify.ready();

    const missing = [];
    const checkProps = (where, schema) => {
      if (!schema) return;
      [].concat(schema.oneOf || [], schema.anyOf || [], schema).forEach(item => {
        Object.entries(item.properties || {}).forEach(([key, prop]) => {
          if (!prop.description) missing.push(`${where}.${key}`);
          if (prop.type === 'object') checkProps(`${where}.${key}`, prop);
        });
      });
    };
    const taskRoutes = routes.filter(route => route.url.startsWith('/api/task/'));
    taskRoutes.forEach(route => {
      const { schema = {} } = route;
      const name = `${route.method} ${route.url}`;
      if (!(schema.tags && schema.tags.length)) missing.push(`${name} tags`);
      if (!schema.summary) missing.push(`${name} summary`);
      if (!(schema.response && schema.response[200] && schema.response[200].description)) missing.push(`${name} response`);
      checkProps(`${name} body`, schema.body);
      checkProps(`${name} query`, schema.querystring || schema.query);
      checkProps(`${name} params`, schema.params);
    });
    await fastify.close();
    expect(taskRoutes.length).to.equal(10);
    expect(missing).to.deep.equal([]);
  });
});

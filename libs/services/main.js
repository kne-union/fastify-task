const fp = require('fastify-plugin');
const getTaskServiceContext = require('../helpers/task-service-context');

module.exports = fp(async (fastify, options) => {
  getTaskServiceContext(fastify, options);
  await fastify.register(require('./executor'), options);
  await fastify.register(require('./scheduler'), options);
  await fastify.register(require('./task'), options);
  await fastify.register(require('./callback'), options);
  await fastify.register(require('./append'), options);
});

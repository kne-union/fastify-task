module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  return next({ secret: 'test-secret' });
};

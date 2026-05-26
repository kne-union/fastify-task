module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  const { type } = task;
  if (type === 'test-type') {
    return { result: 'success' };
  }
  throw new Error(`Unknown task type: ${type}`);
};

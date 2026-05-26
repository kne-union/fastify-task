module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  return await polling(async () => {
    return { result: 'failed', message: 'Polling task failed' };
  });
};

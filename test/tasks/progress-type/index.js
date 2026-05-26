module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  await updateProgress(50);
  return { result: 'success' };
};

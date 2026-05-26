module.exports = async (fastify, options, { task, updateProgress, polling, next, log }) => {
  await log({ message: 'Task log entry', data: { step: 1 } });
  return { result: 'success' };
};

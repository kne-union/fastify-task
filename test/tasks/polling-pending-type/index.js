module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  let callCount = 0;
  return await polling(async () => {
    callCount++;
    if (callCount < 3) {
      return { result: 'pending', data: { callCount } };
    }
    return { result: 'success', data: { callCount } };
  });
};

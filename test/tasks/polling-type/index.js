let callCount = 0;
module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  callCount++;
  if (callCount % 2 === 1) {
    // 第一次调用返回 pending，触发 polling
    return polling(async () => {
      return { result: 'success', data: { value: 42 } };
    });
  }
  return { result: 'success' };
};

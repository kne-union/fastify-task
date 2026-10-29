module.exports = async (fastify, options, { task, updateProgress, polling, next }) => {
  const mode = task.input && task.input.mode;
  if (mode === 'progress') {
    await updateProgress(80);
    return { result: 'success' };
  }
  if (mode === 'next') {
    return { nextResult: await next({ changed: true }) };
  }
  if (mode === 'cancel-then-throw') {
    task.status = 'canceled';
    throw new Error('boom after cancel');
  }
  if (mode === 'throw-without-stack') {
    throw 'plain failure';
  }
  return polling(async () => ({ result: 'pending', progress: 30 }), { pollInterval: 1, maxPollTimes: 3 });
};

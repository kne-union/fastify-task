const fp = require('fastify-plugin');
const fs = require('fs-extra');
const getTaskServiceContext = require('../helpers/task-service-context');

module.exports = fp(async (fastify, options) => {
  const context = getTaskServiceContext(fastify, options);

  const append = async ({ name = context.mainNamespace, dir, dirs, scriptName, tasks, override = false }) => {
    const appendDirs = context.normalizeDirs({ dir, dirs });
    const result = { dirs: [], tasks: [], skippedTasks: [] };
    for (const currentDir of appendDirs) {
      if (!options.dirs.includes(currentDir)) {
        if (!(await fs.exists(currentDir))) {
          console.warn(`append 目录不存在:${currentDir}，仍会添加但运行时可能无法匹配任务执行器`);
        }
        // 保持旧 API 可见性；执行器会优先使用任务声明自己的 dirs。
        options.dirs.push(currentDir);
        result.dirs.push(currentDir);
      }
    }
    if (tasks && typeof tasks === 'object') {
      Object.entries(tasks).forEach(([taskName, config]) => {
        const { skipped, declaration } = context.registerTaskDeclaration({
          namespace: name,
          taskName,
          dirs: appendDirs.length > 0 ? appendDirs : options.dirs,
          scriptName: scriptName || 'index',
          config,
          override,
          syncLegacyTask: true
        });
        if (skipped) {
          result.skippedTasks.push(declaration.type);
          return;
        }
        result.tasks.push(declaration.type);
      });
    }
    return result;
  };

  Object.assign(fastify[options.name].services, {
    append
  });
});

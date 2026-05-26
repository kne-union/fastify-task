-- 新增字段（幂等）
ALTER TABLE tasks ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN tasks.priority IS '任务优先级，数值越大越优先';

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS parent_task_id UUID;
COMMENT ON COLUMN tasks.parent_task_id IS '父任务ID，用于任务依赖/链式执行';

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN tasks.retry_count IS '已重试次数';

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS max_retries INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN tasks.max_retries IS '最大重试次数，0表示不自动重试';

ALTER TABLE tasks ADD COLUMN IF NOT EXISTS completed_user_id UUID;
COMMENT ON COLUMN tasks.completed_user_id IS '完成任务的用户ID';

-- 修改字段默认值
ALTER TABLE tasks ALTER COLUMN input SET DEFAULT NULL;
ALTER TABLE tasks ALTER COLUMN output SET DEFAULT NULL;

-- 新增索引（幂等）
CREATE INDEX IF NOT EXISTS idx_tasks_parent_task_id ON tasks (parent_task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks (priority);

-- 新增外键（幂等）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_tasks_parent_task'
  ) THEN
ALTER TABLE tasks ADD CONSTRAINT fk_tasks_parent_task
    FOREIGN KEY (parent_task_id) REFERENCES tasks (id);
END IF;
END $$;
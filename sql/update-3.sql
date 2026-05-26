-- 新增字段（幂等）
ALTER TABLE t_task ADD COLUMN IF NOT EXISTS "priority" INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN t_task."priority" IS '任务优先级，数值越大越优先';

ALTER TABLE t_task ADD COLUMN IF NOT EXISTS "parent_task_id" BIGINT;
COMMENT ON COLUMN t_task."parent_task_id" IS '父任务ID，用于任务依赖/链式执行';

ALTER TABLE t_task ADD COLUMN IF NOT EXISTS "retry_count" INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN t_task."retry_count" IS '已重试次数';

ALTER TABLE t_task ADD COLUMN IF NOT EXISTS "max_retries" INTEGER NOT NULL DEFAULT 0;
COMMENT ON COLUMN t_task."max_retries" IS '最大重试次数，0表示不自动重试';

ALTER TABLE t_task ADD COLUMN IF NOT EXISTS "completed_user_id" BIGINT;
COMMENT ON COLUMN t_task."completed_user_id" IS '完成任务的用户ID';

-- 修改字段默认值
ALTER TABLE t_task ALTER COLUMN "input" SET DEFAULT NULL;
ALTER TABLE t_task ALTER COLUMN "output" SET DEFAULT NULL;

-- 新增索引（幂等）
CREATE INDEX IF NOT EXISTS idx_t_task_parent_task_id ON t_task ("parent_task_id");
CREATE INDEX IF NOT EXISTS idx_t_task_priority ON t_task ("priority");

-- 新增外键（幂等）
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'fk_t_task_parent_task'
  ) THEN
ALTER TABLE t_task ADD CONSTRAINT fk_t_task_parent_task
    FOREIGN KEY ("parent_task_id") REFERENCES t_task (id);
END IF;
END $$;
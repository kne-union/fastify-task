-- 为 tasks 表添加 startedAt 字段
DO
$$
BEGIN
    IF
NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 't_task'
        AND column_name = 'started_at'
    ) THEN
ALTER TABLE t_task
    ADD COLUMN "started_at" TIMESTAMP WITH TIME ZONE;

COMMENT
ON COLUMN t_task."started_at" IS '任务实际开始执行时间';
END IF;
END $$;

-- 兼容旧数据：将已完成任务的 created_at 作为 startedAt 的回退值
UPDATE t_task
SET "started_at" = created_at
WHERE "started_at" IS NULL
  AND status IN ('running', 'success', 'failed', 'canceled');
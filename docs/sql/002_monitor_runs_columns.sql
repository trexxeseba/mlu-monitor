-- ============================================================================
-- 002_monitor_runs_columns.sql
-- Agrega columnas faltantes a monitor_runs si ya existía con esquema antiguo.
-- Idempotente — se puede correr múltiples veces sin riesgo.
-- Correr en Supabase: SQL Editor → pegar → Run
-- ============================================================================

ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS invalid_reason    TEXT;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS finished_at       TIMESTAMPTZ;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS sellers_total     INTEGER DEFAULT 0;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS sellers_ok        INTEGER DEFAULT 0;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS sellers_failed    INTEGER DEFAULT 0;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS total_items       INTEGER DEFAULT 0;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS prev_valid_run_id TEXT;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS prev_total_items  INTEGER;
ALTER TABLE monitor_runs ADD COLUMN IF NOT EXISTS items_delta_pct   NUMERIC(6, 2);

-- Índices (idempotentes)
CREATE INDEX IF NOT EXISTS idx_monitor_runs_status_finished ON monitor_runs (status, finished_at DESC);
CREATE INDEX IF NOT EXISTS idx_monitor_runs_run_id ON monitor_runs (run_id);

-- Verificación
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'monitor_runs'
ORDER BY ordinal_position;

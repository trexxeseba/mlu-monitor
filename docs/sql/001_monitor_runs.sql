-- ============================================================================
-- 001_monitor_runs.sql
-- Migración NO destructiva — se puede correr múltiples veces sin riesgo.
-- NO borra ni modifica datos existentes.
-- Correr en Supabase: SQL Editor → pegar → Run
-- ============================================================================


-- ─── 1. Tabla monitor_runs ────────────────────────────────────────────────────
-- Registra cada ejecución del monitor con su estado de validez.
-- El detector solo compara runs con status = 'valid'.

CREATE TABLE IF NOT EXISTS monitor_runs (
  id                BIGSERIAL   PRIMARY KEY,
  run_id            TEXT        NOT NULL UNIQUE,
  status            TEXT        NOT NULL DEFAULT 'running',
  -- Valores posibles: 'running' | 'valid' | 'invalid'
  invalid_reason    TEXT,
  started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at       TIMESTAMPTZ,
  sellers_total     INTEGER     DEFAULT 0,
  sellers_ok        INTEGER     DEFAULT 0,
  sellers_failed    INTEGER     DEFAULT 0,
  total_items       INTEGER     DEFAULT 0,
  prev_valid_run_id TEXT,
  prev_total_items  INTEGER,
  items_delta_pct   NUMERIC(6, 2),   -- positivo = cayó, negativo = subió
  created_at        TIMESTAMPTZ DEFAULT NOW()
);

COMMENT ON TABLE  monitor_runs                 IS 'Registro de cada ejecución del monitor MLU';
COMMENT ON COLUMN monitor_runs.run_id          IS 'ID único por ejecución: run_<timestamp_ms>';
COMMENT ON COLUMN monitor_runs.status          IS 'running | valid | invalid';
COMMENT ON COLUMN monitor_runs.invalid_reason  IS 'Motivo si status = invalid';
COMMENT ON COLUMN monitor_runs.items_delta_pct IS 'Δ% de items vs run válido previo. Positivo = cayó.';


-- ─── 2. Índices en monitor_runs ───────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_monitor_runs_status_finished
  ON monitor_runs (status, finished_at DESC);

CREATE INDEX IF NOT EXISTS idx_monitor_runs_run_id
  ON monitor_runs (run_id);


-- ─── 3. Índices en snapshots ──────────────────────────────────────────────────
-- Mejoran la performance de las consultas del detector y del monitor.

CREATE INDEX IF NOT EXISTS idx_snapshots_run_id
  ON snapshots (run_id);

CREATE INDEX IF NOT EXISTS idx_snapshots_seller_run
  ON snapshots (seller_id, run_id);

CREATE INDEX IF NOT EXISTS idx_snapshots_item_run
  ON snapshots (item_id, run_id);


-- ─── 4. Columnas adicionales en bajas_detectadas ─────────────────────────────
-- Nuevas columnas para el sistema refactorizado.
-- ADD COLUMN IF NOT EXISTS es idempotente — no falla si ya existen.

ALTER TABLE bajas_detectadas
  ADD COLUMN IF NOT EXISTS detection_run_id TEXT;

ALTER TABLE bajas_detectadas
  ADD COLUMN IF NOT EXISTS stock_anterior INTEGER;

ALTER TABLE bajas_detectadas
  ADD COLUMN IF NOT EXISTS stock_nuevo INTEGER;

COMMENT ON COLUMN bajas_detectadas.detection_run_id IS
  'run_id del run actual donde se detectó el cambio. Usado para deduplicación.';

COMMENT ON COLUMN bajas_detectadas.stock_anterior IS
  'available_quantity en el run válido anterior';

COMMENT ON COLUMN bajas_detectadas.stock_nuevo IS
  'available_quantity en el run válido actual';


-- ─── 5. Verificación final ────────────────────────────────────────────────────

SELECT
  'monitor_runs'      AS tabla,
  COUNT(*)            AS filas_actuales
FROM monitor_runs
UNION ALL
SELECT
  'bajas_detectadas'  AS tabla,
  COUNT(*)            AS filas_actuales
FROM bajas_detectadas;

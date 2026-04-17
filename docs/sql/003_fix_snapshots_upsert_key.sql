-- ============================================================================
-- 003_fix_snapshots_upsert_key.sql
-- Agrega constraint único (seller_id, meli_item_id) en snapshots.
-- Esto permite que el UPSERT de monitor.js funcione por vendor — sin este
-- constraint, si dos sellers comparten un mismo MLU ID (imposible en la práctica
-- pero posible en tests), el UPSERT pisaría la fila del otro seller.
-- Idempotente — se puede correr múltiples veces sin riesgo.
-- Correr en Supabase: SQL Editor → pegar → Run
-- ============================================================================

-- 1. Eliminar posible constraint viejo (solo meli_item_id)
--    Nota: si no existe, el DO bloque simplemente ignora el error.
DO $$
BEGIN
  -- Intentar eliminar constraint antiguo basado solo en meli_item_id
  -- (nombre puede variar; buscamos constraints que incluyan SOLO meli_item_id)
  DECLARE
    rec RECORD;
  BEGIN
    FOR rec IN
      SELECT tc.constraint_name
      FROM information_schema.table_constraints tc
      JOIN information_schema.constraint_column_usage ccu
        ON tc.constraint_name = ccu.constraint_name
       AND tc.table_name = ccu.table_name
      WHERE tc.table_name = 'snapshots'
        AND tc.constraint_type = 'UNIQUE'
        AND ccu.column_name = 'meli_item_id'
    LOOP
      -- Solo eliminar si el constraint es SOLO sobre meli_item_id
      IF (
        SELECT COUNT(*)
        FROM information_schema.constraint_column_usage
        WHERE constraint_name = rec.constraint_name
          AND table_name = 'snapshots'
      ) = 1 THEN
        EXECUTE 'ALTER TABLE snapshots DROP CONSTRAINT IF EXISTS ' || quote_ident(rec.constraint_name);
        RAISE NOTICE 'Eliminado constraint antiguo: %', rec.constraint_name;
      END IF;
    END LOOP;
  END;
END $$;

-- 2. Agregar constraint único compuesto (seller_id, meli_item_id)
--    Si ya existe con este nombre, IF NOT EXISTS lo ignora.
ALTER TABLE snapshots
  ADD CONSTRAINT IF NOT EXISTS snapshots_seller_item_unique
  UNIQUE (seller_id, meli_item_id);

-- 3. Índice de soporte para consultas por seller_id + run_id (detector)
CREATE INDEX IF NOT EXISTS idx_snapshots_seller_run
  ON snapshots (seller_id, run_id);

-- 4. Verificación
SELECT
  tc.constraint_name,
  tc.constraint_type,
  string_agg(ccu.column_name, ', ' ORDER BY ccu.column_name) AS columns
FROM information_schema.table_constraints tc
JOIN information_schema.constraint_column_usage ccu
  ON tc.constraint_name = ccu.constraint_name
 AND tc.table_name = ccu.table_name
WHERE tc.table_name = 'snapshots'
  AND tc.constraint_type IN ('UNIQUE', 'PRIMARY KEY')
GROUP BY tc.constraint_name, tc.constraint_type
ORDER BY tc.constraint_type, tc.constraint_name;

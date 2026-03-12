# Runbook de Operación — MLU Monitor

## Checklist inicial (primera vez)

### 1. Correr la migración SQL (opcional pero recomendado)

En Supabase → SQL Editor:

```sql
-- Pegar el contenido de docs/sql/001_monitor_runs.sql y ejecutar
```

> Si no se corre la migración, el monitor usa `execution_logs` como fallback
> y el detector usa los run_ids de la tabla `snapshots`. El pipeline funciona
> en ambos casos, pero la validación es más robusta con `monitor_runs`.

### 2. Configurar secrets en GitHub

Ir a: `https://github.com/trexxeseba/mlu-monitor/settings/secrets/actions`

Agregar:
- `SUPABASE_URL` — URL de tu proyecto Supabase
- `SUPABASE_KEY` — Service role key (NO la anon key)
- `SCRAPFLY_KEY` — API key de Scrapfly

### 3. Validar al menos 2 runs manuales antes de activar el cron

---

## Prueba manual local

### Requisitos previos

```bash
cd ~/mlu-monitor
npm install
pip install -r requirements.txt
```

Variables de entorno (crear `.env` o exportar):

```bash
export SUPABASE_URL="https://TU-PROYECTO.supabase.co"
export SUPABASE_KEY="tu-service-role-key"
export SCRAPFLY_KEY="tu-scrapfly-key"
```

### Paso 1: Correr el monitor

```bash
node src/monitor.js
```

Verificar en la salida:
- `📋 N sellers activos`
- `✅ X/Y item IDs guardados` por seller
- `Run status: VALID` al final

Si el run aparece como `INVALID`, el validate_run abortará el detector (correcto).

### Paso 2: Correr validate_run

```bash
node src/validate_run.js
```

- Exit 0 + `✅ Run válido` → continuar
- Exit 2 + `❌ RUN INVÁLIDO` → no correr el detector manualmente

### Paso 3: Correr el detector (solo si validate_run exitó 0)

```bash
node src/detector_bajas.js
```

En el primer run: solo habrá `nuevo` para todos los items.
En el segundo run en adelante: aparecerán `desaparecido_no_confirmado`, `reaparecido` y nuevos `nuevo`.

---

## Prueba manual en GitHub Actions

1. Ir a: `https://github.com/trexxeseba/mlu-monitor/actions`
2. Seleccionar workflow **"MLU Monitor Pipeline"**
3. Click **"Run workflow"** → **"Run workflow"** (verde)
4. Esperar a que termine (~5-15 min dependiendo de sellers)
5. Click en el run → ver logs de cada step
6. Descargar artifact `mlu-monitor-run-N` → contiene `monitor.log`, `validate.log`, `detector.log`

### Interpretar los logs

**monitor.log**: buscar
- `Run status: VALID` → OK
- `Run status: INVALID` + motivo → scraping roto o caída de items

**validate.log**: buscar
- `✅ Run válido` → detector corrió
- `❌ RUN INVÁLIDO` → detector no corrió (esperado si scraping falló)

**detector.log**: buscar
- Resumen de cambios por tipo
- `TOTAL: N cambios`

---

## Activar el cron

Solo activar después de ≥2 runs manuales exitosos.

En `.github/workflows/monitor.yml`, descomentar:

```yaml
  # schedule:
  #   - cron: '0 */4 * * *'   # Cada 4 horas
```

→ queda:

```yaml
  schedule:
    - cron: '0 */4 * * *'   # Cada 4 horas
```

Commitear y pushear a `main`.

---

## Monitoreo operacional en Supabase

### Ver los últimos runs

```sql
SELECT run_id, status, sellers_ok, sellers_total, total_items,
       items_delta_pct, invalid_reason, finished_at
FROM monitor_runs
ORDER BY finished_at DESC
LIMIT 20;
```

### Ver últimos cambios detectados

```sql
SELECT tipo, COUNT(*) AS n, MAX(fecha_deteccion) AS ultimo
FROM bajas_detectadas
GROUP BY tipo
ORDER BY n DESC;
```

### Ver items desaparecidos recientemente

```sql
SELECT seller_id, item_id, fecha_deteccion, run_id
FROM bajas_detectadas
WHERE tipo = 'desaparecido_no_confirmado'
ORDER BY fecha_deteccion DESC
LIMIT 50;
```

### Ver items reaparecidos recientemente

```sql
SELECT seller_id, item_id, fecha_deteccion, run_id
FROM bajas_detectadas
WHERE tipo = 'reaparecido'
ORDER BY fecha_deteccion DESC
LIMIT 50;
```

---

## Troubleshooting

### El monitor falla con "Scrapfly: página bloqueada"

- Verificar que `SCRAPFLY_KEY` es válida y tiene créditos
- El seller puede estar bloqueando; Scrapfly con `asp=true` debería manejar esto

### El run se marca inválido por "30% sellers fallaron"

- Revisar si Scrapfly tiene problemas de servicio
- Reducir sellers activos temporalmente para aislar el problema

### El detector no corre aunque el monitor termina bien

- Revisar `validate.log` — puede que el run quedó en estado `running` (crash del monitor)
- Si no existe tabla `monitor_runs`, el fallback usa `execution_logs` — verificar que esa tabla existe

### El detector detecta miles de "nuevo" inesperadamente

- Probable: los runs previos eran inválidos (no los compara) y recién hay 2 runs válidos
- Normal en el primer uso o después de cambios de scope

### Purge de datos viejos

```sql
-- Borrar snapshots de más de 30 días (preservar los de runs válidos recientes)
DELETE FROM snapshots
WHERE checked_at < NOW() - INTERVAL '30 days'
  AND run_id NOT IN (
    SELECT run_id FROM monitor_runs
    WHERE status = 'valid'
    ORDER BY finished_at DESC
    LIMIT 10
  );
```

---

## Rotación de credenciales (urgente si hubo exposición)

Si las credenciales de Supabase estuvieron expuestas (estaban en `download-supabase.js` y `GITHUB_SECRETS_SETUP.md` en texto plano en commits anteriores):

1. Supabase Dashboard → Settings → API → regenerar `service_role` key
2. Actualizar secret `SUPABASE_KEY` en GitHub Actions
3. Actualizar en cualquier entorno local donde uses la key
4. Verificar logs de Supabase para accesos no autorizados

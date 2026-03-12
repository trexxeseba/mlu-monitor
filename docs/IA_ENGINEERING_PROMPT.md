# IA Engineering Prompt — MLU Monitor

Este documento es el contexto canónico para cualquier asistente de IA o ingeniero
que necesite trabajar sobre este sistema.

---

## ¿Qué hace este sistema?

Monitorea los listings de competidores en Mercado Libre Uruguay (MLU).
Cada N horas (actualmente 4h via GitHub Actions):
1. Scrapea el listado de cada seller activo usando Scrapfly
2. Obtiene el detalle de cada item via la API pública de MeLi
3. Guarda snapshots en Supabase
4. Valida si el run fue íntegro (no roto por bloqueo de Scrapfly)
5. Si es válido, compara con el run válido anterior y clasifica cambios

---

## Archivos productivos (NO tocar sin entender)

```
src/monitor.js          — Scraper principal (Node.js)
src/validate_run.js     — Validador de integridad del run (Node.js)
src/detector_bajas.py   — Detector de cambios (Python)
.github/workflows/monitor.yml — Pipeline de GHA
docs/sql/001_monitor_runs.sql — Migración SQL (idempotente)
```

## Archivos legacy (NO usar en producción)

```
legacy/download-supabase.js   — ⚠️ TIENE CREDENCIALES HARDCODEADAS (solo referencia)
legacy/debug_scrape.js        — Script de debug con BrightData API
legacy/workflows/             — Workflows viejos (no activos)
```

---

## Invariantes que NUNCA deben romperse

1. **El detector solo compara runs con `status = 'valid'`**.
   Si se compara contra runs inválidos, se generan falsos positivos masivos.

2. **No hay límite artificial de items por seller**.
   El `slice(0, 50)` fue eliminado en el refactor v2.0. No reintroducirlo.

3. **El workflow NO commitea logs al repo**.
   Los logs van como artifacts de GitHub Actions. No agregar pasos de `git commit`.

4. **Las credenciales van en GitHub Secrets, NUNCA en código**.
   `SUPABASE_URL`, `SUPABASE_KEY`, `SCRAPFLY_KEY`.

5. **El cron está desactivado por defecto**.
   Se activa manualmente después de validar al menos 2 runs locales.

---

## Lógica de validez del run

Un run es `invalid` si:
- Más del **30%** de los sellers fallaron en el scraping
- El total de items cayó más del **40%** vs el último run válido previo
- Un seller devolvió menos del **50%** de su baseline de items → ese seller se marca `failed`

Un run `invalid` no es un error técnico — es un estado esperado cuando Scrapfly
está bloqueado o hay problemas de red. El pipeline sigue corriendo pero el detector
no ejecuta, evitando clasificar items como "desaparecidos" cuando en realidad
el scraping simplemente falló.

---

## Clasificación de cambios

| Tipo                      | Señal                              | Prioridad |
|---------------------------|------------------------------------|-----------|
| `vendido_confirmado`      | sold_quantity subió                | Alta — corta el análisis del item |
| `vendido_probable`        | available_quantity bajó, activo    | Media     |
| `desaparecido_no_confirmado` | item no aparece en run actual   | Media     |
| `nuevo`                   | item aparece por primera vez       | Baja      |
| `status_cambio`           | status cambió                      | Media     |
| `precio_cambio`           | precio cambió ≥ 5%                 | Baja      |
| `stock_cambio`            | available_quantity cambió, sin venta | Baja   |

---

## Cómo agregar un seller nuevo

```sql
INSERT INTO sellers (seller_id, nombre_real, nickname, activo)
VALUES ('12345678', 'Nombre Empresa', 'nick_meli', true);
```

En el próximo run, el seller será scrapeado. En el primer run válido solo tendrá
items clasificados como `nuevo` (no hay baseline previo). Desde el segundo run
en adelante se detectarán cambios reales.

---

## Cómo interpretar `desaparecido_no_confirmado`

No es sinónimo de "vendido". Puede ser:
- Item vendido (si el seller lo elimina/cierra al vender)
- Item pausado por el seller (voluntary)
- Item eliminado por MeLi (violación de políticas)
- Fallo temporal de scraping (aunque el sistema intenta prevenirlo con validación del run)

Para confirmar una venta: buscar si hay `vendido_confirmado` previo para el mismo `item_id`.

---

## Cómo debuggear un run inválido

1. Revisar `monitor.log` (artifact del run):
   - ¿Cuántos sellers fallaron?
   - ¿Cuál fue el motivo de fallo de cada uno?
2. Consultar en Supabase:
   ```sql
   SELECT * FROM monitor_runs ORDER BY finished_at DESC LIMIT 5;
   ```
3. Si Scrapfly está bloqueando: esperar y correr manualmente más tarde.
4. Si el problema es un seller específico: marcarlo como `activo = false` temporalmente.

---

## Variables de entorno necesarias

| Variable      | Dónde se define                  |
|---------------|----------------------------------|
| SUPABASE_URL  | GitHub Secrets + local .env      |
| SUPABASE_KEY  | GitHub Secrets + local .env      |
| SCRAPFLY_KEY  | GitHub Secrets + local .env      |

---

## Riesgos conocidos y pendientes

| Riesgo                              | Severidad | Estado      |
|-------------------------------------|-----------|-------------|
| Credenciales en commits anteriores  | CRÍTICO   | Rotar urgente |
| Rate limit MeLi API sin auth        | Medio     | Sin solución actual |
| Scrapfly bloqueo esporádico         | Medio     | Mitigado con validez del run |
| Tabla snapshots crece sin purge     | Bajo      | Ver RUNBOOK para purge |
| `api.mercadolibre.com.uy` (dominio) | Bajo      | Verificar que devuelve 200 |

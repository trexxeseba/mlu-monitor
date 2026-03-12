# Arquitectura — MLU Monitor

## Resumen del sistema

Sistema de monitoreo de competidores en Mercado Libre Uruguay.
Detecta aparición y desaparición de publicaciones por seller,
minimizando falsos positivos mediante validación de integridad de cada run antes de detectar.

**Alcance actual**: el monitor extrae únicamente los item_ids presentes en el listado de cada seller.
No consulta precio, stock ni status vía API externa.

---

## Stack

| Componente   | Tecnología                       |
|--------------|----------------------------------|
| Scraping     | Scrapfly API (render_js, asp)    |
| Base de datos| Supabase (PostgreSQL)            |
| Orquestación | GitHub Actions                   |
| Monitor      | Node.js 20 (`src/monitor.js`)    |
| Validación   | Node.js 20 (`src/validate_run.js`) |
| Detección    | Node.js 20 (`src/detector_bajas.js`)  |

---

## Tablas en Supabase

### `sellers`
Sellers a monitorear. Cada fila = un competidor.

| Campo        | Tipo    | Descripción                      |
|--------------|---------|----------------------------------|
| seller_id    | TEXT    | ID de MeLi del vendedor          |
| nombre_real  | TEXT    | Nombre legible (display)         |
| nickname     | TEXT    | Nick en MeLi                     |
| activo       | BOOLEAN | Si está activo en el monitor     |

### `snapshots`
Un snapshot por item por run. Contiene solo los item_ids extraídos del listado.

| Campo        | Tipo        | Descripción                        |
|--------------|-------------|------------------------------------|
| run_id       | TEXT        | Qué run generó este snapshot       |
| seller_id    | INTEGER     | A qué seller pertenece             |
| item_id      | TEXT        | ID del item MLU                    |
| meli_item_id | TEXT        | Mismo que item_id                  |
| checked_at   | TIMESTAMPTZ | Momento del snapshot               |
| timestamp    | TIMESTAMPTZ | Alias de checked_at                |

Columnas presentes en tabla pero no pobladas por el monitor actual:
`title`, `price`, `sold_quantity`, `available_quantity`, `status`, `url`, `thumbnail`

### `monitor_runs`
Un registro por ejecución del monitor. Permite al detector saber qué runs son confiables.

| Campo              | Tipo        | Descripción                              |
|--------------------|-------------|------------------------------------------|
| run_id             | TEXT        | Identificador único (`run_<timestamp>`)  |
| status             | TEXT        | running / valid / invalid                |
| invalid_reason     | TEXT        | Motivo si status = invalid               |
| sellers_total      | INTEGER     | Sellers procesados                       |
| sellers_ok         | INTEGER     | Sellers con scraping exitoso             |
| sellers_failed     | INTEGER     | Sellers que fallaron                     |
| total_items        | INTEGER     | Total de item_ids guardados en este run  |
| prev_valid_run_id  | TEXT        | Run válido anterior (referencia)         |
| prev_total_items   | INTEGER     | Items del run válido anterior            |
| items_delta_pct    | NUMERIC     | Δ% de items (positivo = cayó)            |
| finished_at        | TIMESTAMPTZ | Cuándo terminó                           |

### `bajas_detectadas`
Cambios detectados por el detector. Cada fila = un evento de cambio.

| Campo            | Tipo        | Descripción                              |
|------------------|-------------|------------------------------------------|
| seller_id        | INTEGER     | Seller involucrado (NOT NULL)            |
| item_id          | TEXT        | Item involucrado                         |
| meli_item_id     | TEXT        | Alias de item_id                         |
| tipo             | TEXT        | Tipo de cambio (ver tabla abajo)         |
| run_id           | TEXT        | Run donde se detectó                     |
| fecha_deteccion  | TIMESTAMPTZ | Cuándo se detectó                        |

---

## Tipos de cambio

| Tipo                         | Condición                                                         |
|------------------------------|-------------------------------------------------------------------|
| `nuevo`                      | Item aparece en run actual pero no en el anterior (primer avistamiento) |
| `desaparecido_no_confirmado` | Item estaba en el anterior y ya no aparece en el actual           |
| `reaparecido`                | Item vuelve a aparecer habiendo sido registrado como desaparecido |

---

## Flujo del pipeline

```
GitHub Actions (workflow_dispatch o cron cada 4h)
    │
    ▼
1. src/monitor.js
   ├── getActiveSellers()           ← tabla sellers
   ├── getLastValidRun()            ← tabla monitor_runs (fallback: execution_logs)
   ├── Para cada seller:
   │   ├── scrapeSellerIds()        ← Scrapfly API (render_js, asp, country=uy)
   │   ├── Baseline check vs run válido anterior
   │   └── saveSnapshots(itemIds)   ← tabla snapshots (solo item_ids)
   ├── Validación del run:
   │   ├── ¿Más del 30% sellers fallaron?   → status = invalid
   │   └── ¿Items cayeron más del 40%?      → status = invalid
   └── upsertRun()                 ← tabla monitor_runs (fallback: execution_logs)
       │
       ├── Exit 0 (siempre, salvo FATAL)
       │
    ▼
2. src/validate_run.js
   ├── Lee el run más reciente de monitor_runs (fallback: execution_logs)
   ├── Exit 0  → run válido, continuar
   ├── Exit 1  → error técnico (tabla no existe, DB caída)
   └── Exit 2  → run inválido → detector NO corre
       │
       └─ (si exit 0) ──────────────────────────┐
                                                 ▼
                                    3. src/detector_bajas.js
                                       ├── get_last_two_valid_runs()
                                       ├── get_item_ids_for_run(current)
                                       ├── get_item_ids_for_run(previous)
                                       ├── detect_changes()     ← set difference
                                       └── guardar_cambios()    ← bajas_detectadas
```

---

## Umbrales de validez del run

| Umbral                        | Valor | Motivo                               |
|-------------------------------|-------|--------------------------------------|
| MAX_SELLER_FAIL_RATIO         | 30%   | Si más sellers fallan, el run es ruidoso |
| MAX_ITEMS_DROP_RATIO          | 40%   | Caída brusca → Scrapfly probablemente bloqueado |
| MIN_SELLER_ITEMS_RATIO        | 50%   | Seller con <50% de su baseline → fallo de scraping |

---

## Secrets requeridos en GitHub

| Secret        | Descripción                          |
|---------------|--------------------------------------|
| SUPABASE_URL  | URL del proyecto Supabase            |
| SUPABASE_KEY  | Service role key de Supabase         |
| SCRAPFLY_KEY  | API key de Scrapfly                  |

**IMPORTANTE**: Las credenciales NUNCA deben estar en código fuente.
Ver `GITHUB_SECRETS_SETUP.md` para instrucciones de configuración.

---

## Consideraciones de escala

- **Scrapfly**: cada seller = 1 request de scraping. Con render_js activo, consumo ~5-10 créditos/request.
- **Sin API de MeLi**: el monitor no hace requests a MeLi por item — sin rate limiting ni riesgo de bloqueo de esa fuente.
- **Supabase**: cada run inserta N_sellers × avg_items filas en snapshots (solo item_ids). Monitorear tamaño de tabla y considerar purge de runs viejos (>30 días).
- **Cron cada 4h**: 6 runs/día. Ajustar según consumo de créditos Scrapfly.

# Arquitectura — MLU Monitor

## Resumen del sistema

Sistema de monitoreo de competidores en Mercado Libre Uruguay.
Detecta ventas, cambios de precio, variaciones de stock y aparición/desaparición de items,
minimizando falsos positivos mediante validación de integridad de cada run antes de detectar.

---

## Stack

| Componente   | Tecnología                       |
|--------------|----------------------------------|
| Scraping     | Scrapfly API (render_js, asp)    |
| Item details | MeLi API pública (`api.mercadolibre.com`) |
| Base de datos| Supabase (PostgreSQL)            |
| Orquestación | GitHub Actions                   |
| Monitor      | Node.js 20 (`src/monitor.js`)    |
| Validación   | Node.js 20 (`src/validate_run.js`) |
| Detección    | Python 3.11 (`src/detector_bajas.py`) |

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
Un snapshot por item por run. Crecimiento esperado: N_sellers × avg_items × runs_por_dia.

| Campo              | Tipo        | Descripción                        |
|--------------------|-------------|------------------------------------|
| run_id             | TEXT        | Qué run generó este snapshot       |
| seller_id          | TEXT        | A qué seller pertenece             |
| item_id            | TEXT        | ID del item MLU                    |
| meli_item_id       | TEXT        | Mismo que item_id (legado)         |
| price              | NUMERIC     | Precio al momento del snapshot     |
| available_quantity | INTEGER     | Stock disponible                   |
| sold_quantity      | INTEGER     | Acumulado de unidades vendidas     |
| status             | TEXT        | active / paused / closed           |
| checked_at         | TIMESTAMPTZ | Momento del snapshot               |

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
| total_items        | INTEGER     | Total de items guardados en este run     |
| prev_valid_run_id  | TEXT        | Run válido anterior (referencia)         |
| prev_total_items   | INTEGER     | Items del run válido anterior            |
| items_delta_pct    | NUMERIC     | Δ% de items (positivo = cayó)            |
| finished_at        | TIMESTAMPTZ | Cuándo terminó                           |

### `bajas_detectadas`
Cambios detectados por el detector. Cada fila = un evento de cambio.

| Campo              | Tipo        | Descripción                              |
|--------------------|-------------|------------------------------------------|
| seller_id          | TEXT        | Seller involucrado                       |
| item_id            | TEXT        | Item involucrado                         |
| tipo               | TEXT        | Tipo de cambio (ver tabla abajo)         |
| detection_run_id   | TEXT        | Run actual donde se detectó              |
| fecha_deteccion    | TIMESTAMPTZ | Cuándo se detectó                        |

---

## Tipos de cambio

| Tipo                      | Condición                                                    |
|---------------------------|--------------------------------------------------------------|
| `nuevo`                   | Item aparece en run actual pero no en el anterior            |
| `desaparecido_no_confirmado` | Item estaba y ya no aparece (puede ser venta, pausa, cierre) |
| `vendido_confirmado`      | `sold_quantity` aumentó (señal más fuerte — fin de análisis para ese item) |
| `vendido_probable`        | `available_quantity` bajó y el item sigue `active`           |
| `status_cambio`           | Cambió el campo `status` (e.g., active → paused)             |
| `precio_cambio`           | Precio cambió ≥ 5%                                           |
| `stock_cambio`            | `available_quantity` cambió sin venta confirmada ni probable |

---

## Flujo del pipeline

```
GitHub Actions (workflow_dispatch o cron cada 4h)
    │
    ▼
1. src/monitor.js
   ├── getActiveSellers()           ← tabla sellers
   ├── getLastValidRun()            ← tabla monitor_runs
   ├── Para cada seller:
   │   ├── scrapeSellerIds()        ← Scrapfly API
   │   ├── Baseline check vs run válido anterior
   │   ├── fetchItemDetail() × N   ← MeLi API (sin límite)
   │   └── saveSnapshots()         ← tabla snapshots
   ├── Validación del run:
   │   ├── ¿Más del 30% sellers fallaron?   → status = invalid
   │   └── ¿Items cayeron más del 40%?      → status = invalid
   └── upsertRun()                 ← tabla monitor_runs
       │
       ├── Exit 0 (siempre, salvo FATAL)
       │
    ▼
2. src/validate_run.js
   ├── Lee el run más reciente de monitor_runs
   ├── Exit 0  → run válido, continuar
   ├── Exit 1  → error técnico (tabla no existe, DB caída)
   └── Exit 2  → run inválido → detector NO corre
       │
       └─ (si exit 0) ──────────────────────────┐
                                                 ▼
                                    3. src/detector_bajas.py
                                       ├── get_last_two_valid_runs()
                                       ├── get_snapshots_for_run(current)
                                       ├── get_snapshots_for_run(previous)
                                       ├── detect_changes()
                                       └── guardar_cambios()  ← bajas_detectadas
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
- **MeLi API**: 1 request por item. Sin auth = rate limit público (~1 req/seg implícito).
- **Supabase**: cada run inserta N_sellers × avg_items filas en snapshots. Monitorear tamaño de tabla y considerar purge de runs viejos (>30 días).
- **Cron cada 4h**: 6 runs/día. Ajustar según consumo de créditos Scrapfly.

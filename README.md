# MLU Monitor v2.0

Monitor robusto de competidores en **Mercado Libre Uruguay**.
Detecta ventas, cambios de precio, variaciones de stock y desaparición de items,
con validación de integridad de cada run para minimizar falsos positivos.

---

## Arquitectura

```
GitHub Actions (manual o cron cada 4h)
    │
    ├─ 1. src/monitor.js       → Scrapea sellers, guarda snapshots en Supabase
    ├─ 2. src/validate_run.js  → Verifica que el run no esté roto
    └─ 3. src/detector_bajas.py → Compara run válido actual vs anterior
                                   Guarda cambios en bajas_detectadas
```

Ver: [`docs/CTO_ARCHITECTURE_MELI_MONITOR.md`](docs/CTO_ARCHITECTURE_MELI_MONITOR.md)

---

## Primeros pasos

### 1. Correr la migración SQL (una sola vez)

En Supabase → SQL Editor → pegar `docs/sql/001_monitor_runs.sql` → Run.

### 2. Configurar secrets en GitHub

`Settings → Secrets and variables → Actions`:

| Secret        | Descripción                     |
|---------------|---------------------------------|
| SUPABASE_URL  | URL del proyecto Supabase       |
| SUPABASE_KEY  | Service role key de Supabase    |
| SCRAPFLY_KEY  | API key de Scrapfly             |

### 3. Validar manualmente antes de activar el cron

```bash
npm install
pip install -r requirements.txt

export SUPABASE_URL="..."
export SUPABASE_KEY="..."
export SCRAPFLY_KEY="..."

node src/monitor.js
node src/validate_run.js    # debe salir con código 0
python3 src/detector_bajas.py
```

### 4. Activar el cron (cuando ≥2 runs manuales sean exitosos)

En `.github/workflows/monitor.yml`, descomentar:
```yaml
  schedule:
    - cron: '0 */4 * * *'   # Cada 4 horas
```

---

## Scripts disponibles

```bash
npm run monitor    # node src/monitor.js
npm run validate   # node src/validate_run.js
npm run detector   # python3 src/detector_bajas.py
```

---

## Tipos de cambio detectados

| Tipo                         | Descripción                                      |
|------------------------------|--------------------------------------------------|
| `vendido_confirmado`         | `sold_quantity` aumentó entre runs               |
| `vendido_probable`           | `available_quantity` bajó, item sigue activo     |
| `desaparecido_no_confirmado` | Item no aparece en el run actual                 |
| `nuevo`                      | Item aparece por primera vez                     |
| `precio_cambio`              | Precio cambió ≥ 5%                               |
| `stock_cambio`               | Stock cambió sin venta confirmada                |
| `status_cambio`              | Campo status cambió (active/paused/closed)       |

---

## Estructura del repositorio

```
mlu-monitor/
├── src/
│   ├── monitor.js           ← Scraper principal
│   ├── validate_run.js      ← Validador de integridad del run
│   └── detector_bajas.py    ← Detector de cambios
├── docs/
│   ├── sql/
│   │   └── 001_monitor_runs.sql
│   ├── CTO_ARCHITECTURE_MELI_MONITOR.md
│   ├── RUNBOOK_OPERACION.md
│   └── IA_ENGINEERING_PROMPT.md
├── .github/
│   └── workflows/
│       └── monitor.yml      ← Pipeline principal (cron desactivado)
├── legacy/                  ← Archivos viejos, NO usar en producción
├── output/                  ← Logs locales (ignorados por git)
├── package.json
└── requirements.txt
```

---

## Seguridad

> ⚠️  **Commits anteriores contienen credenciales de Supabase en texto plano**
> (`legacy/download-supabase.js` y `legacy/GITHUB_SECRETS_SETUP.md`).
> **Rotar la service role key inmediatamente** si el repo es público o fue clonado por terceros.
>
> Supabase Dashboard → Settings → API → regenerar `service_role` key → actualizar GitHub Secret `SUPABASE_KEY`.

---

## Documentación adicional

- [Arquitectura técnica](docs/CTO_ARCHITECTURE_MELI_MONITOR.md)
- [Runbook operacional](docs/RUNBOOK_OPERACION.md)
- [Contexto para IA](docs/IA_ENGINEERING_PROMPT.md)
- [Setup de secrets](GITHUB_SECRETS_SETUP.md)

# MLU Monitor v2.0

Monitor de competidores en **Mercado Libre Uruguay**.
Detecta aparición, desaparición y reaparición de publicaciones por seller,
con validación de integridad de cada run para minimizar falsos positivos.

> **Alcance**: el monitor extrae únicamente item_ids del listado de cada seller vía Scrapfly.
> No depende de la API de MeLi.

---

## Arquitectura

```
GitHub Actions (manual o cron cada 4h)
    │
    ├─ 1. src/monitor.js        → Scrapea sellers, guarda snapshots en Supabase
    ├─ 2. src/validate_run.js   → Verifica que el run no esté roto
    └─ 3. src/detector_bajas.js → Compara run válido actual vs anterior
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

export SUPABASE_URL="..."
export SUPABASE_KEY="..."
export SCRAPFLY_KEY="..."

node src/monitor.js
node src/validate_run.js     # debe salir con código 0
node src/detector_bajas.js
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
npm run detector   # node src/detector_bajas.js
```

---

## Tipos de cambio detectados

| Tipo                         | Descripción                                                         |
|------------------------------|---------------------------------------------------------------------|
| `nuevo`                      | Item aparece en run actual pero no en el anterior                   |
| `desaparecido_no_confirmado` | Item estaba en el anterior y ya no aparece en el actual             |
| `reaparecido`                | Item vuelve a aparecer habiendo sido registrado como desaparecido   |

> El monitor extrae únicamente item_ids del listado scrapeado.
> No consulta precio, stock ni status vía API de MeLi.

---

## Estructura del repositorio

```
mlu-monitor/
├── src/
│   ├── monitor.js           ← Scraper principal
│   ├── validate_run.js      ← Validador de integridad del run
│   └── detector_bajas.js    ← Detector de cambios
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

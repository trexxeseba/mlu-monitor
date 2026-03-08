# MLU Monitor — Sheets Sync

Sincroniza datos de Supabase a Google Sheets cada 2 horas.

## Setup (primera vez)

### 1. Clonar & instalar
```bash
cd C:\Users\undia\mlu-monitor
npm install
```

### 2. Crear Google Service Account
- Google Cloud Console → proyecto `clauditaaa`
- IAM → Service Accounts → `mlu-monitor-sheets`
- Generar JSON key → descargar como `clauditaaa-dbcde137b8d8.json`
- Guardar en esta carpeta

### 3. Compartir Google Sheet
- Copiar `client_email` del JSON: `mlu-monitor-sheets@clauditaaa.iam.gserviceaccount.com`
- Ir a tu Google Sheet
- Compartir con ese email (Editor)

### 4. Configurar Windows Task Scheduler
```powershell
# Run como Administrator en PowerShell
Set-ExecutionPolicy -ExecutionPolicy Bypass -Scope CurrentUser
C:\Users\undia\mlu-monitor\schedule-2h.ps1
```

Eso crea una tarea que corre cada 2 horas automáticamente.

## Archivos

- `monitor.js` — scraper original (cada 6h via Task Scheduler)
- `sheets-sync.js` — sync a Google Sheets (cada 2h)
- `clauditaaa-dbcde137b8d8.json` — credenciales Google (NO compartir públicamente)
- `schedule-2h.ps1` — setup del scheduler
- `run-sheets-sync.ps1` — wrapper que ejecuta sheets-sync.js

## Google Sheets

Tu sheet tiene 3 hojas:

### RESUMEN
- Vendedor ID, Nickname
- Total items, Items vendidos
- % de venta
- Último update

### PRODUCTOS
- Seller ID, Item ID, Título, Precio
- Estado (active, sold, etc)
- Last seen

### TIMELINE
- Cambios recientes por fecha
- Tipo de cambio (disappeared, appeared, etc)

## Logs

Los logs de cada ejecución van en:
```
C:\Users\undia\mlu-monitor\logs\sheets-sync-YYYY-MM-DD.log
```

## Parar la tarea

```powershell
Unregister-ScheduledTask -TaskName "MLU Monitor Sheets Sync (2h)" -Confirm:$false
```

## Debug

Ejecutar manualmente:
```powershell
C:\Users\undia\mlu-monitor\run-sheets-sync.ps1
```

O directamente:
```bash
node C:\Users\undia\mlu-monitor\sheets-sync.js
```

# C:\Users\undia\mlu-monitor\schedule-2h.ps1
# Run this ONCE to set up the 2-hour task, then delete it

$taskName = "MLU Monitor Sheets Sync (2h)"
$taskPath = "\MLU Monitor\"
$scriptPath = "C:\Users\undia\mlu-monitor\run-sheets-sync.ps1"
$logPath = "C:\Users\undia\mlu-monitor\logs\sheets-sync-$(Get-Date -Format 'yyyy-MM-dd').log"

# Create logs directory if needed
if (!(Test-Path "C:\Users\undia\mlu-monitor\logs")) {
  New-Item -ItemType Directory -Path "C:\Users\undia\mlu-monitor\logs" | Out-Null
}

# Create the action
$action = New-ScheduledTaskAction `
  -Execute "powershell.exe" `
  -Argument "-ExecutionPolicy Bypass -File `"$scriptPath`" >> `"$logPath`" 2>&1"

# Create the trigger (every 2 hours)
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Hours 2) -RepetitionDuration (New-TimeSpan -Days 365)

# Register the task
Register-ScheduledTask `
  -TaskName $taskName `
  -Action $action `
  -Trigger $trigger `
  -RunLevel Highest `
  -Description "MLU Monitor - Sync Supabase to Google Sheets every 2 hours" `
  -Force

Write-Host "[✓] Task creado: $taskName"
Write-Host "[✓] Corre cada 2 horas"
Write-Host "[✓] Logs en: $logPath"

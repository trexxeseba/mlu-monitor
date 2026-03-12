$action = New-ScheduledTaskAction `
    -Execute 'C:\Program Files\nodejs\node.exe' `
    -Argument 'C:\Users\undia\mlu-monitor\monitor.js' `
    -WorkingDirectory 'C:\Users\undia\mlu-monitor'

$trigger = New-ScheduledTaskTrigger `
    -RepetitionInterval (New-TimeSpan -Hours 6) `
    -Once `
    -At '00:00'

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 1) `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable

Register-ScheduledTask `
    -TaskName 'MLU-Monitor' `
    -Description 'Monitorea listings de vendedores en MercadoLibre Uruguay cada 6 horas' `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Force

Write-Host "Tarea creada. Verificando..."
schtasks /Query /TN "MLU-Monitor" /FO LIST

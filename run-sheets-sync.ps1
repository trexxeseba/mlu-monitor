# C:\Users\undia\mlu-monitor\run-sheets-sync.ps1

$dir = "C:\Users\undia\mlu-monitor"
Set-Location $dir

Write-Host "[→] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') — Iniciando Sheets Sync..."

# Run the Node.js script
node "$dir\sheets-sync.js"

if ($LASTEXITCODE -eq 0) {
  Write-Host "[✓] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') — Sync completado"
} else {
  Write-Host "[✗] $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') — Error en sync (exit code: $LASTEXITCODE)"
  exit 1
}

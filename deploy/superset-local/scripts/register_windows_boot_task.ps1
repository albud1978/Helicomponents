$ErrorActionPreference = "Stop"

$scriptPath = "C:\ProgramData\SupersetBoot\windows_boot_recover.ps1"
if (-not (Test-Path $scriptPath)) {
    throw "Script not found: $scriptPath"
}

$action = New-ScheduledTaskAction -Execute "powershell.exe" -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
$trigger = New-ScheduledTaskTrigger -AtStartup
$trigger.Delay = "PT40S"
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName "SupersetRecoverAtBoot" -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force
Write-Output "Task SupersetRecoverAtBoot registered."

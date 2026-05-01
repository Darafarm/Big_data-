# =============================================================
# start_reactor.ps1
# One-command morning startup script
#
# What it does:
#   1. Starts EC2 instance
#   2. Waits 90 seconds for boot
#   3. Gets new IP address automatically
#   4. Updates watcher script with new IP
#   5. Prints dashboard URL and connection info
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File "start_reactor.ps1"
# =============================================================

$instanceId    = "i-084fd00b0356d1b12"
$watcherScript = "C:\Users\daram\ReactorProject\scripts\watch_and_upload.ps1"

Write-Host "=== REACTOR SYSTEM STARTUP ===" -ForegroundColor Cyan

# Step 1: Start EC2
Write-Host "Starting EC2 instance..." -ForegroundColor Yellow
aws ec2 start-instances --instance-ids $instanceId | Out-Null

# Step 2: Wait for boot
Write-Host "Waiting 90 seconds for EC2 to boot..." -ForegroundColor Yellow
Start-Sleep -Seconds 90

# Step 3: Get new IP
Write-Host "Getting new IP address..." -ForegroundColor Yellow
$newIP = aws ec2 describe-instances `
    --instance-ids $instanceId `
    --query "Reservations[*].Instances[*].PublicIpAddress" `
    --output text
Write-Host "New IP: $newIP" -ForegroundColor Green

# Step 4: Update watcher script with new IP
Write-Host "Updating watcher script..." -ForegroundColor Yellow
$content = Get-Content $watcherScript
$updated = $content -replace '\d+\.\d+\.\d+\.\d+', $newIP
$updated | Set-Content $watcherScript
Write-Host "Watcher script updated: $newIP" -ForegroundColor Green

# Step 5: Print connection info
Write-Host ""
Write-Host "=== READY ===" -ForegroundColor Green
Write-Host "Dashboard : http://$newIP`:8050"
Write-Host "PuTTY     : ubuntu@$newIP"
Write-Host "Key file  : C:\Users\daram\ReactorProject\reactor-key-final.ppk"
Write-Host ""
Write-Host "Next steps:"
Write-Host "  1. Connect PuTTY to ubuntu@$newIP"
Write-Host "  2. Run: start-dfs.sh && start-yarn.sh"
Write-Host "  3. Run: nohup python3 ~/dashboard.py > ~/dashboard.log 2>&1 &"
Write-Host "  4. Start watcher: powershell -ExecutionPolicy Bypass -File `"$watcherScript`""

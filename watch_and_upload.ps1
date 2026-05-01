
# watch_and_upload.ps1
# Windows folder watcher — monitors incoming_csv for new data
#
# What it does:
#   1. Watches incoming_csv\ folder every 2 seconds
#   2. When new FOLDER detected:
#      - Merges all CSVs inside into one combined file
#      - Uploads combined file to S3 Bronze
#      - Triggers pipeline on EC2 via SSH
#      - Moves folder to processed_csv\
#   3. When new CSV FILE detected:
#      - Uploads directly to S3 Bronze
#      - Triggers pipeline on EC2 via SSH
#
# Usage:
#   powershell -ExecutionPolicy Bypass -File "watch_and_upload.ps1"
#
# IMPORTANT: Update ec2IP every morning — it changes on EC2 restart


# ---- CONFIGURATION --------------------------------------------
$watchFolder     = "C:\Users\daram\ReactorProject\incoming_csv"
$processedFolder = "C:\Users\daram\ReactorProject\processed_csv"
$s3Bucket        = "s3://reactor-project-emperor1/bronze/"
$logFile         = "C:\Users\daram\ReactorProject\logs\upload_log.txt"
$ec2User         = "ubuntu"
$ec2IP           = "REPLACE-WITH-TODAYS-EC2-IP"
$keyFile         = "C:\Users\daram\ReactorProject\reactor-key-final.ppk"

# ---- LOGGING FUNCTION -----------------------------------------
function Log($msg) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $line = "$timestamp  $msg"
    Write-Host $line
    try { Add-Content -Path $logFile -Value $line } catch {}
}

# ---- PROCESS A SINGLE CSV FILE --------------------------------
function Process-CSVFile($filePath, $fileName) {
    Log "Uploading $fileName to S3 Bronze..."
    aws s3 cp $filePath $s3Bucket

    if ($LASTEXITCODE -eq 0) {
        Log "Upload successful: $fileName"
        Log "Triggering pipeline on EC2..."
        $cmd = "/home/ubuntu/run_pipeline.sh $fileName"
        ssh -i $keyFile -o StrictHostKeyChecking=no "${ec2User}@${ec2IP}" $cmd
        Log "Pipeline complete: $fileName"
    } else {
        Log "ERROR: Upload failed for $fileName"
    }
}

# ---- PROCESS A FOLDER OF CSV FILES ----------------------------
# Merges all CSVs in the folder into one combined file first
function Process-Folder($folderPath, $folderName) {
    $csvFiles = Get-ChildItem -Path $folderPath -Filter "*.csv" -Recurse
    Log "Found $($csvFiles.Count) CSV files in folder $folderName"

    $combinedName = "${folderName}_combined.csv"
    $combinedPath = "$watchFolder\$combinedName"
    $header       = $null
    $allRows      = @()

    foreach ($csv in $csvFiles) {
        Log "Reading: $($csv.Name)"
        $lines = Get-Content -Path $csv.FullName -Encoding Unicode
        if ($null -eq $header) {
            $header = $lines[0]   # Take header from first file only
        }
        $allRows += $lines[1..($lines.Count - 1)]   # Skip header in rest
    }

    # Write merged file
    @($header) + $allRows | Set-Content -Path $combinedPath -Encoding UTF8
    Log "Merged $($csvFiles.Count) files into $combinedName ($($allRows.Count) rows)"

    # Upload and trigger pipeline
    Process-CSVFile $combinedPath $combinedName

    # Move folder to processed to prevent reprocessing
    $dest = "$processedFolder\$folderName"
    Move-Item -Path $folderPath -Destination $dest -Force
    Log "Folder moved to processed: $folderName"
    Log "-------------------------------------------"
}

# ---- START WATCHING -------------------------------------------
Log "=========================================="
Log "Reactor Watcher Started (Folder Mode)"
Log "Watching: $watchFolder"
Log "Handles: individual CSVs and folders of CSVs"
Log "=========================================="
Log "Waiting for new CSV files or folders..."

$watcher                      = New-Object System.IO.FileSystemWatcher
$watcher.Path                 = $watchFolder
$watcher.Filter               = "*"
$watcher.IncludeSubdirectories = $false
$watcher.EnableRaisingEvents  = $true

# Main loop — polls every 2 seconds
while ($true) {
    $result = $watcher.WaitForChanged(
        [System.IO.WatcherChangeTypes]::Created, 2000)

    if (-not $result.TimedOut) {
        $itemName = $result.Name
        $itemPath = Join-Path $watchFolder $itemName

        # Is it a FOLDER?
        if (Test-Path $itemPath -PathType Container) {
            Start-Sleep -Seconds 5   # Wait for copy to complete
            Process-Folder $itemPath $itemName

        # Is it a CSV FILE?
        } elseif ($itemName -like "*.csv") {
            Start-Sleep -Seconds 3
            Process-CSVFile $itemPath $itemName
            Move-Item $itemPath "$processedFolder\$itemName" -Force
            Log "File moved to processed: $itemName"
            Log "-------------------------------------------"
        }
    }
}

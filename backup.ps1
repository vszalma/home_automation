function Get-NewestBackupDate {
    param (
        [string]$BackupRootFolder = "C:\Backups"
    )

    # Check if the root backup folder exists
    if (-not (Test-Path -Path $BackupRootFolder)) {
        Write-Error "The backup folder path '$BackupRootFolder' does not exist."
        return $null
    }

    # Get all subfolders and filter for names matching the yyyyMMdd pattern
    $backupDates = Get-ChildItem -Path $BackupRootFolder -Directory |
        Where-Object { $_.Name -match '^\d{8}$' } |
        ForEach-Object {
            try {
                # Convert the folder name to a DateTime object
                [datetime]::ParseExact($_.Name, 'yyyyMMdd', $null)
            } catch {
                # Skip folders that do not match the date format
                $null
            }
        }

    # Return the newest date if any valid dates were found
    if ($backupDates.Count -gt 0) {
        return ($backupDates | Sort-Object -Descending)[0]
    } else {
        Write-Output "No valid backup folders found."
        return $null
    }
}


function Start-BackupProcess {
    # Define the source and destination paths
    $source = "C:\SourceFolder"
    $destination = "D:\DestinationFolder"
    $logFile = "C:\robocopy_log.txt"

    # Build the robocopy arguments
    $copyArgs = "$source $destination /e /mt:6 /xo /nfl /ndl /log:$logFile"

    # Execute robocopy using Start-Process
    Start-Process -FilePath "robocopy" -ArgumentList $copyArgs -NoNewWindow -Wait

}

# Define the directory path and the time threshold (1 week ago)
$directoryPath = "C:\Users\vszal\OneDrive\Documents"
$lastBackupDate = Get-NewestBackupDate

# Check if $lastBackupDate is null or not a valid date
if (-not $lastBackupDate -or -not ($lastBackupDate -is [datetime])) {
    Write-Output "Invalid or null date detected. Setting default date."
    $lastBackupDate = [datetime]::ParseExact("19000101", 'yyyyMMdd', $null)
} else {
    Write-Output "Valid date detected: $lastBackupDate"
}

# Get the most recent modification date of any file in the directory and its subdirectories
$lastModifiedDate = Get-ChildItem -Path $directoryPath -Recurse -File |
    Sort-Object -Property LastWriteTime -Descending |
    Select-Object -First 1 -ExpandProperty LastWriteTime

# Check if the most recent modification date is older than 1 week
if ($lastModifiedDate -gt $lastBackupDate) {
    Write-Output "The last modification date ($lastModifiedDate) is newer than the last backup. Backup starting."
} else {
    Write-Output "The last modification date ($lastModifiedDate) older than the last backup. No backup done."
}

.\validate.ps1 -sourceDir "C:\Users\vszal\Downloads" -destinationDir "C:\Users\vszal\Documents"
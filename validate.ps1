param (
    [string]$Source,
    [string]$Destination,
    [bool]$Hash = $false
)


function Compare-Directories {

    # Check if both directories exist
    if (-not (Test-Path -Path $Source) -or -not (Test-Path -Path $Destination)) {
        Write-Error "One or both of the directories do not exist."
        return
    }

    # Get file count and total size for Dir1
    $SourceFiles = Get-ChildItem -Path $Source -Recurse -File
    $SourceFileCount = $SourceFiles.Count
    $SourceTotalSize = ($SourceFiles | Measure-Object -Property Length -Sum).Sum

    # Get file count and total size for Dir2
    $DestinationFiles = Get-ChildItem -Path $Destination -Recurse -File
    $DestinationFileCount = $DestinationFiles.Count
    $DestinationTotalSize = ($DestinationFiles | Measure-Object -Property Length -Sum).Sum

    # Create the output string
    $output = @"
Comparison of Directories:

Source Directory: $Source
    File Count: $SourceFileCount
    Total Size: $([math]::Round($SourceTotalSize / 1MB, 2)) MB

Destination Directory: $Destination
    File Count: $DestinationFileCount
    Total Size: $([math]::Round($DestinationTotalSize / 1MB, 2)) MB

"@

    # Compare the results and append to the output string
    if ($SourceFileCount -eq $DestinationFileCount -and $SourceTotalSize -eq $DestinationTotalSize) {
        $output += "Result: The directories have the same file count and total size.`n"
    } else {
        $output += "Result: The directories differ in file count and/or total size.`n"
    }

    # Output the formatted string using Out-String
    $output | Out-String
}

function Compare-DirectoryHashes {

    # Get file hashes for the source directory
    $sourceHashes = Get-ChildItem -Path $Source -Recurse -File |
        Get-FileHash |
        Select-Object Hash, Path

    # Get file hashes for destination directory
    $destinationHashes = Get-ChildItem -Path $Destination -Recurse -File |
        Get-FileHash |
        Select-Object Hash, Path

    # Compare the hash lists
    $difference = Compare-Object -ReferenceObject $sourceHashes -DifferenceObject $destinationHashes -Property Hash, Path

    if ($difference.Count -eq 0) {
        Write-Output "The directories have identical files."
    } else {
        Write-Output "The directories have different files:"
        $difference | Format-Table
    }
}


if ($Hash) {
    Compare-DirectoryHashes
}
else {
    Compare-Directories
}

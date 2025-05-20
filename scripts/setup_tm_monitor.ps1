<#
.SYNOPSIS
  Create, schedule, start and later stop a Windows Performance Monitor Data Collector Set for long‐running model runs,
  then convert the resulting .blg file to CSV for analysis.

.DESCRIPTION
  - Creates a Data Collector Set named $CollectorName under User-Defined with your chosen set of counters.
  - Schedules it to start at $StartTime and stop after $DurationDays days.
  - Starts the collector immediately if $StartImmediately is $true.
  - After collection finishes (logman stops it automatically), you can re-run this script with –ConvertOnly to export the .blg to .csv.

.PARAMETER CollectorName
  Name of the data collector set (must be unique).

.PARAMETER OutputDirectory
  Directory where the .blg (and later the .csv) will be written.

.PARAMETER SampleIntervalSeconds
  How often to sample each counter (default 15s).

.PARAMETER StartTime
  When to auto-start the collector (default: now).

.PARAMETER DurationDays
  How many days to run before auto-stop.

.PARAMETER StartImmediately
  If $true, also kicks off the collector right now.

.PARAMETER ConvertOnly
  If $true, skips creation/scheduling and only runs the relog conversion on the existing .blg.

.EXAMPLE
  .\Setup-TravelModelMonitor.ps1
  Create & start immediately, run for 4 days (default), output under C:\PerfLogs\TravelModelMonitor.

.EXAMPLE
  .\Setup-TravelModelMonitor.ps1 -ConvertOnly
  Convert the existing .blg to .csv and exit.
#>

param(
  [string]$CollectorName        = "TravelModelMonitor",
  [string]$OutputDirectory      = "C:\PerfLogs\TravelModelMonitor",
  [int]   $SampleIntervalSeconds = 15,
  [datetime]$StartTime          = (Get-Date),
  [int]   $DurationDays         = 4,
  [switch]$StartImmediately     = $true,
  [switch]$ConvertOnly
)

# Paths
$blgFile = Join-Path $OutputDirectory "$CollectorName.blg"
$csvFile = Join-Path $OutputDirectory "$CollectorName.csv"

if ($ConvertOnly) {
    if (!(Test-Path $blgFile)) {
        Write-Error "BLG file not found at $blgFile"
        exit 1
    }
    Write-Host "Converting $blgFile → $csvFile ..."
    relog $blgFile -f CSV -o $csvFile
    Write-Host "Done. CSV available at $csvFile"
    exit 0
}

# 1) Prepare output folder
if (!(Test-Path $OutputDirectory)) {
    New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
    Write-Host "Created directory $OutputDirectory"
}

# 2) Remove existing collector (if any)
$existing = logman query userdefined | Select-String "^$CollectorName$"
if ($existing) {
    Write-Host "Deleting existing collector set '$CollectorName' ..."
    logman delete $CollectorName
}

# 3) Define your counters
$counters = @(
    "\Processor(_Total)\% Processor Time",
    "\System\Processor Queue Length",
    "\System\Context Switches/sec",
    "\Memory\Available MBytes",
    "\Memory\Pages/sec",
    "\Paging File(_Total)\% Usage",
    "\PhysicalDisk(_Total)\Avg. Disk sec/Read",
    "\PhysicalDisk(_Total)\Avg. Disk sec/Write",
    "\PhysicalDisk(_Total)\Current Disk Queue Length",
    "\Network Interface(*)\Bytes Total/sec"
)

# 4) Create & schedule the Data Collector Set
Write-Host "Creating Data Collector Set '$CollectorName' ..."
logman create counter $CollectorName `
    -f BIN `
    -o $blgFile `
    -c $counters `
    -si $SampleIntervalSeconds `
    -b $($StartTime.ToString("MM/dd/yyyy HH:mm:ss")) `
    -e $($StartTime.AddDays($DurationDays).ToString("MM/dd/yyyy HH:mm:ss"))

Write-Host "Scheduled to start at $($StartTime) and stop at $($StartTime.AddDays($DurationDays))."

# 5) Optionally start immediately
if ($StartImmediately) {
    Write-Host "Starting '$CollectorName' now..."
    logman start $CollectorName
    Write-Host "Collector started. It will auto-stop after $DurationDays day(s)."
} else {
    Write-Host "Collector created but not started. To start it now, run:"
    Write-Host "  logman start $CollectorName"
}

Write-Host ""
Write-Host "— After your run finishes, open Performance Monitor → Reports → User Defined → $CollectorName → Latest"
Write-Host "— Or convert the .blg to CSV by rerunning this script with the –ConvertOnly flag."

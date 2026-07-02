# finance-pi: deploy to the Raspberry Pi from this Windows machine.
#
# Usage (from the repo root or anywhere):
#   powershell -File ops\deploy-from-windows.ps1                # deploy + verify
#   powershell -File ops\deploy-from-windows.ps1 -FullRebuild   # + one-time full rebuild
#   powershell -File ops\deploy-from-windows.ps1 -Pit           # + fundamentals_pit rebuild
#   powershell -File ops\deploy-from-windows.ps1 -SetupKey      # one-time SSH key install
#
# The server-side steps live in ops/deploy.sh; this wrapper pipes it over SSH,
# so it works even before the script lands on the server via git.
param(
    [string]$Target = "cantabile@192.168.68.84",
    [switch]$FullRebuild,
    [switch]$Pit,
    [switch]$SkipTests,
    [switch]$NoRestart,
    [switch]$Stash,
    [switch]$SetupKey
)

$ErrorActionPreference = "Stop"
# PS 5.1 pipes to native programs using $OutputEncoding (ASCII by default).
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)

$scriptPath = Join-Path $PSScriptRoot "deploy.sh"
if (-not (Test-Path $scriptPath)) { throw "deploy.sh not found at $scriptPath" }

if ($SetupKey) {
    $keyFile = Join-Path $env:USERPROFILE ".ssh\id_ed25519"
    if (-not (Test-Path "$keyFile.pub")) {
        Write-Host "Generating an SSH key ($keyFile)..."
        ssh-keygen -t ed25519 -N '""' -f $keyFile
    }
    Write-Host "Installing the public key on $Target (enter the server password once)..."
    Get-Content "$keyFile.pub" | ssh $Target "mkdir -p ~/.ssh; cat >> ~/.ssh/authorized_keys; chmod 700 ~/.ssh; chmod 600 ~/.ssh/authorized_keys"
    if ($LASTEXITCODE -ne 0) { throw "key install failed (exit $LASTEXITCODE)" }
    Write-Host "Done. Future deploys will not ask for a password."
    exit 0
}

$deployArgs = @()
if ($FullRebuild) { $deployArgs += "--full-rebuild" }
if ($Pit)         { $deployArgs += "--pit" }
if ($SkipTests)   { $deployArgs += "--skip-tests" }
if ($NoRestart)   { $deployArgs += "--no-restart" }
if ($Stash)       { $deployArgs += "--stash" }
$argLine = $deployArgs -join " "

Write-Host "Deploying to $Target $argLine"
# Strip CR so the script survives Windows CRLF checkouts.
$script = (Get-Content $scriptPath -Raw) -replace "`r", ""
$script | ssh $Target "bash -s -- $argLine"
if ($LASTEXITCODE -ne 0) { throw "deployment failed (exit $LASTEXITCODE)" }
Write-Host "Deployment finished successfully."

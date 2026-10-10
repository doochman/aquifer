<#
.SYNOPSIS
  Checks what AQUIFER needs on Windows and, with -Install, installs what is missing.
.DESCRIPTION
  Never installs silently: each winget step is shown and confirmed (skip with -Yes).
  Docker Desktop with the WSL 2 backend is required; enabling WSL 2 needs an administrator
  prompt and usually a restart, which this script explains rather than forces.
.EXAMPLE
  .\setup.cmd            # check only
  .\setup.cmd -Install   # install missing prerequisites with winget (asks first)
#>
param(
  [switch]$Install,
  [switch]$Yes
)

$ErrorActionPreference = 'Continue'
Set-Location -Path $PSScriptRoot
$script:Problems = 0

function Write-Ok($m)   { Write-Host "  [ ok ] $m" -ForegroundColor Green }
function Write-Warn($m) { Write-Host "  [warn] $m" -ForegroundColor Yellow }
function Write-Bad($m)  { Write-Host "  [FAIL] $m" -ForegroundColor Red; $script:Problems++ }
function Test-Command($name) { return [bool](Get-Command $name -ErrorAction SilentlyContinue) }
function Confirm-Step($question) {
  if ($Yes) { return $true }
  $answer = Read-Host "  $question [y/N]"
  return $answer -match '^(y|yes)$'
}

Write-Host "AQUIFER setup check (Windows)" -ForegroundColor Cyan

Write-Host "`nPython"
$python = $null
foreach ($candidate in @('py', 'python')) {
  if (Test-Command $candidate) {
    $pyArgs = if ($candidate -eq 'py') { @('-3') } else { @() }
    & $candidate @pyArgs -c "import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)" 2>$null
    if ($LASTEXITCODE -eq 0) { $python = $candidate; break }
  }
}
if ($python) { Write-Ok "Python found ($python)" } else { Write-Bad "Python 3.8 or newer not found"; $needPython = $true }

Write-Host "`nDocker"
$needDocker = $false
if (Test-Command 'docker') {
  Write-Ok ("docker CLI: " + (docker --version))
  $composeVersion = (docker compose version --short 2>$null)
  if ($composeVersion) {
    Write-Ok "docker compose $composeVersion"
    $parts = ($composeVersion -replace '^v', '').Split('.')
    if ([int]$parts[0] -lt 2 -or ([int]$parts[0] -eq 2 -and [int]$parts[1] -lt 20)) { Write-Bad "Compose 2.20 or newer is required (update Docker Desktop)" }
  } else { Write-Bad "Docker Compose v2 not found (update Docker Desktop)" }
  docker info *> $null
  if ($LASTEXITCODE -eq 0) {
    $memory = [math]::Round((docker info --format '{{.MemTotal}}') / 1GB, 1)
    Write-Ok "Docker daemon is reachable ($memory GB memory for containers)"
    if ($memory -lt 7) { Write-Warn "Docker has under 7 GB: raise it in Docker Desktop > Settings > Resources, or set WSL memory in %UserProfile%\.wslconfig" }
  } else {
    Write-Bad "Docker daemon is not reachable: start Docker Desktop and wait until it is running"
  }
} else {
  Write-Bad "Docker is not installed"
  $needDocker = $true
}

Write-Host "`nOptional"
if (Test-Command 'git') { Write-Ok "git found" } else { Write-Warn "git not found (only needed to clone and contribute)" }

if ($needDocker -or $needPython) {
  Write-Host "`nMissing prerequisites" -ForegroundColor Cyan
  Write-Host "  winget install -e --id Docker.DockerDesktop"
  Write-Host "  winget install -e --id Python.Python.3.12"
  Write-Host "  Docker Desktop needs WSL 2. If it asks for it, run in an administrator PowerShell:  wsl --install   (then restart)"
  if ($Install) {
    if (-not (Test-Command 'winget')) {
      Write-Warn "winget not found. Install 'App Installer' from the Microsoft Store, or download Docker Desktop and Python manually (docs\INSTALL.md)."
    } else {
      if ($needDocker -and (Confirm-Step "Install Docker Desktop with winget?")) { winget install -e --id Docker.DockerDesktop }
      if ($needPython -and (Confirm-Step "Install Python 3.12 with winget?")) { winget install -e --id Python.Python.3.12 }
      Write-Host "  Restart the terminal (and Docker Desktop) so the new programs are found, then run setup.cmd again."
    }
  }
  Write-Host "`n  Details: docs\INSTALL.md"
  exit 1
}

if ($script:Problems -gt 0) {
  Write-Host "`nFix the items above, then run setup.cmd again. See docs\INSTALL.md." -ForegroundColor Yellow
  exit 1
}

Write-Host "`nProject"
& "$PSScriptRoot\lakehouse.cmd" init
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
Write-Host "`nAll set. Next:" -ForegroundColor Green
Write-Host "  .\lakehouse.cmd quickstart      # start everything, load the demo, open the home page"
Write-Host "  .\lakehouse.cmd doctor          # re-check ports and memory any time"

# bin/run.ps1
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# --- Locate project root (parent of /bin) ---
$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ProjectRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $ProjectRoot

# --- Load .env if present ---
if (Test-Path ".env") {
  Get-Content ".env" | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $kv = $line -split "=", 2
    if ($kv.Count -eq 2) {
      $name  = $kv[0].Trim()
      $value = $kv[1].Trim()
      Set-Item -Path "Env:$name" -Value $value
    }
  }
}

# --- Defaults if not set in .env ---
if (-not $env:SERVER_HOST -or $env:SERVER_HOST.Trim() -eq "") { $env:SERVER_HOST = "0.0.0.0" }
if (-not $env:SERVER_PORT -or $env:SERVER_PORT.Trim() -eq "") { $env:SERVER_PORT = "8000" }

# --- Prefer Python from current venv ---
$pyExe = $null
if ($env:VIRTUAL_ENV) {
  $cand = Join-Path $env:VIRTUAL_ENV "Scripts\python.exe"
  if (Test-Path $cand) { $pyExe = $cand }
}
if (-not $pyExe) {
  foreach ($cand in @("python", "python3", "py")) {
    if (Get-Command $cand -ErrorAction SilentlyContinue) { $pyExe = $cand; break }
  }
}
if (-not $pyExe) { Write-Error "Python tidak ditemukan. Aktivasi venv atau install Python."; exit 1 }

# --- Sanity check ---
if (-not (Test-Path "main.py")) {
  Write-Error "main.py tidak ditemukan di $ProjectRoot"
  exit 1
}

# --- Run server (pakai python dari venv kalau ada) ---
& $pyExe -m uvicorn main:app --host $env:SERVER_HOST --port $env:SERVER_PORT

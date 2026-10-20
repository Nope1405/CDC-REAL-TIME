<#
.SYNOPSIS
  Windows entry point mirroring the Makefile (Windows PowerShell 5.1 compatible).

.EXAMPLE
  .\dev.ps1 up
  .\dev.ps1 seed
  .\dev.ps1 chaos 02
  .\dev.ps1 rebuild 2
#>
param(
    [Parameter(Position = 0)] [string] $Target = "help",
    [Parameter(Position = 1)] [string] $Arg = ""
)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

$Py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$JmxVersion = "1.0.1"
$JmxJar = "connect\jmx-exporter\jmx_prometheus_javaagent.jar"

function Invoke-Checked {
    param([string] $Exe, [string[]] $Arguments)
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "$Exe $($Arguments -join ' ') failed with exit code $LASTEXITCODE" }
}

function Find-Bash {
    $candidates = @(
        "$env:ProgramFiles\Git\bin\bash.exe",
        "${env:ProgramFiles(x86)}\Git\bin\bash.exe"
    )
    foreach ($c in $candidates) { if (Test-Path -LiteralPath $c) { return $c } }
    $cmd = Get-Command bash -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    throw "bash not found: install Git for Windows or run the script from WSL"
}

function Ensure-Env {
    if (-not (Test-Path -LiteralPath ".env")) {
        Copy-Item ".env.example" ".env"
        Write-Host "Created .env from .env.example - review the passwords."
    }
}

function Py { param([string[]] $Arguments) Invoke-Checked $Py $Arguments }

switch ($Target) {
    "venv" {
        Invoke-Checked "python" @("-m", "venv", ".venv")
        Py @("-m", "pip", "install", "-r", "consumer\requirements-dev.txt", "-r", "benchmark\requirements.txt")
    }
    "up" {
        Ensure-Env
        Invoke-Checked "docker" @("compose", "up", "-d", "--build")
        Invoke-Checked "docker" @("compose", "ps")
    }
    "down"         { Invoke-Checked "docker" @("compose", "--profile", "monitoring", "--profile", "bench", "down") }
    "reset"        { Invoke-Checked "docker" @("compose", "--profile", "monitoring", "--profile", "bench", "down", "-v") }
    "ps"           { Invoke-Checked "docker" @("compose", "ps") }
    "logs"         { Invoke-Checked "docker" @("compose", "logs", "-f", "--tail=100", "consumer", "kafka-connect") }
    "register"     { Invoke-Checked "docker" @("compose", "run", "--rm", "connect-init") }
    "topics"       { Invoke-Checked "docker" @("compose", "run", "--rm", "kafka-init") }
    "es-bootstrap" { Invoke-Checked "docker" @("compose", "run", "--rm", "es-init") }
    "seed"         { Py @("benchmark\seed.py", "--products", "100000", "--orders", "10000") }
    "test" {
        Push-Location consumer
        try { Invoke-Checked $Py @("-m", "pytest") } finally { Pop-Location }
    }
    "verify"       { Py @("benchmark\verify_consistency.py", "--wait", "120") }
    "bench-single" { Py @("benchmark\load_update.py", "single-tx", "--count", "10000") }
    "bench-pertx"  { Py @("benchmark\load_update.py", "per-tx", "--count", "10000") }
    "bench-rate"   { Py @("benchmark\load_update.py", "rate", "--rate", "500", "--duration", "20") }
    "probe"        { Py @("benchmark\measure_lag.py", "probe", "--count", "200") }
    "report" {
        $run = Get-ChildItem "results\*-load-*.json" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
        if (-not $run) { throw "no results\*-load-*.json yet; run a bench-* target first" }
        Py @("benchmark\measure_lag.py", "report", "--run", $run.FullName)
    }
    "pgbench" {
        Invoke-Checked "docker" @("compose", "--profile", "bench", "up", "-d", "postgres-baseline")
        Py @("benchmark\seed.py", "--port-var", "POSTGRES_BASELINE_PORT", "--orders", "0")
        Invoke-Checked (Find-Bash) @("benchmark/pgbench/compare.sh", "60", "8")
    }
    "jmx-download" {
        $url = "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/$JmxVersion/jmx_prometheus_javaagent-$JmxVersion.jar"
        Invoke-WebRequest -Uri $url -OutFile $JmxJar -UseBasicParsing
        Write-Host "Saved $JmxJar. Now set CONNECT_KAFKA_OPTS in .env (see connect\jmx-exporter\README.md)."
    }
    "monitoring"   { Invoke-Checked "docker" @("compose", "--profile", "monitoring", "up", "-d") }
    "dashboard"    { Py @("monitoring\grafana\build_dashboard.py") }
    "rebuild" {
        $v = if ($Arg) { $Arg } else { "2" }
        Invoke-Checked (Find-Bash) @("benchmark/chaos/08-rebuild-index.sh", $v)
    }
    "chaos" {
        if (-not $Arg) { throw "usage: .\dev.ps1 chaos <NN>   e.g. .\dev.ps1 chaos 02" }
        $script = Get-ChildItem "benchmark\chaos\$Arg-*.sh" | Select-Object -First 1
        if (-not $script) { throw "no benchmark\chaos\$Arg-*.sh" }
        Invoke-Checked (Find-Bash) @("benchmark/chaos/$($script.Name)")
    }
    default {
        Write-Host @"
Usage: .\dev.ps1 <target> [arg]

  venv          host virtualenv (tests + benchmark scripts)
  up            start core stack, create topics/indices, register connectors
  down | reset  stop (reset also deletes volumes)
  ps | logs     status / follow consumer + connect logs
  register      re-register connectors     topics        re-create topics
  es-bootstrap  re-install ES templates    seed          100k products + 10k orders
  test          consumer unit tests        verify        compare PG and ES
  bench-single | bench-pertx | bench-rate  load scenarios
  probe         commit->searchable probes  report        per-stage lag of last run
  pgbench       pgbench baseline vs CDC vs paused
  monitoring    Prometheus + Grafana       jmx-download  Debezium JMX exporter jar
  dashboard     regenerate Grafana JSON    rebuild [V]   replay topic into products_vV
  chaos <NN>    run benchmark\chaos\NN-*.sh
"@
    }
}

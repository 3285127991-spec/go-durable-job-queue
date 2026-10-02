# Repeatable CLI demo for durablequeue.
# Every dq invocation is a separate process, so this also demonstrates
# that all state survives process restarts. Safe to run repeatedly:
# the demo data directory is wiped at the start.
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
$env:GOCACHE = Join-Path $root ".gocache"

Write-Host "== build ==" -ForegroundColor Cyan
go build -o bin\dq.exe ./cmd/dq

$dir = "data-cli-demo"
Remove-Item -Recurse -Force $dir -ErrorAction SilentlyContinue

function Step($msg) { Write-Host "`n== $msg ==" -ForegroundColor Cyan }

Step "enqueue task-a (max-attempts 2) and task-b (max-attempts 1)"
.\bin\dq.exe enqueue -dir $dir -id task-a -payload "send-email" -max-attempts 2
.\bin\dq.exe enqueue -dir $dir -id task-b -payload "charge-card" -max-attempts 1

Step "idempotent enqueue: task-a again"
.\bin\dq.exe enqueue -dir $dir -id task-a -payload "different-payload"

Step "claim task-a in a new process, then ack it"
$claim = .\bin\dq.exe claim -dir $dir | ConvertFrom-Json
Write-Host "claimed $($claim.id) token=$($claim.token.Substring(0,8))..."
.\bin\dq.exe ack -dir $dir -id $claim.id -token $claim.token

Step "claim task-b and fail it -> dead letter queue"
$claim = .\bin\dq.exe claim -dir $dir | ConvertFrom-Json
.\bin\dq.exe fail -dir $dir -id $claim.id -token $claim.token

Step "dead letter queue"
.\bin\dq.exe list -dir $dir -status dead

Step "compact (snapshot + truncate log)"
.\bin\dq.exe compact -dir $dir

Step "stats after restart (new process, state recovered from disk)"
.\bin\dq.exe stats -dir $dir

Write-Host "`nCLI demo finished OK" -ForegroundColor Green

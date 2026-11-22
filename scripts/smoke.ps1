# PowerShell smoke test equivalent.
$ErrorActionPreference = "Stop"

function Invoke-Cleanup {
    docker compose down -v *> $null
}

try {
    # 1. clean slate
    docker compose down -v

    # 2. bring up cluster
    docker compose up -d

    # 3. wait for healthchecks (up to 30s)
    $deadline = (Get-Date).AddSeconds(30)
    while ($true) {
        $healthy = (docker compose ps --format '{{.Health}}' | Select-String -Pattern 'healthy' | Measure-Object).Count
        if ($healthy -ge 5) { break }
        if ((Get-Date) -gt $deadline) {
            throw "timeout waiting for nodes to become healthy"
        }
        Start-Sleep -Seconds 2
    }

    # 4. put
    $putOut = (docker compose run --rm gateway put smoke-key "hello" --mode rs32 2>$null)
    $putOut | ForEach-Object { Write-Host $_ }
    if ($putOut -notmatch '"ok": true') { throw "put failed" }

    # 5. get round-trip
    $getOut = (docker compose run --rm gateway get smoke-key 2>$null)
    $getOut | ForEach-Object { Write-Host $_ }
    if ($getOut -notmatch '"found": true') { throw "get failed" }
    if ($getOut -notmatch '"value": "hello"') { throw "value mismatch" }

    # 6. kill one node
    docker compose stop node3 | Out-Null

    # 7. get still works with one node down
    $getOut = (docker compose run --rm gateway get smoke-key 2>$null)
    $getOut | ForEach-Object { Write-Host $_ }
    if ($getOut -notmatch '"found": true') { throw "get failed (1 node down)" }
    if ($getOut -notmatch '"value": "hello"') { throw "value mismatch (1 node down)" }

    # 8. kill a second node
    docker compose stop node4 | Out-Null

    # 9. get still works with two nodes down
    $getOut = (docker compose run --rm gateway get smoke-key 2>$null)
    $getOut | ForEach-Object { Write-Host $_ }
    if ($getOut -notmatch '"found": true') { throw "get failed (2 nodes down)" }
    if ($getOut -notmatch '"value": "hello"') { throw "value mismatch (2 nodes down)" }

    # 10. restart stopped nodes
    docker compose start node3 node4 | Out-Null

    # 11. wait for healthy
    $deadline = (Get-Date).AddSeconds(30)
    while ($true) {
        $healthy = (docker compose ps --format '{{.Health}}' | Select-String -Pattern 'healthy' | Measure-Object).Count
        if ($healthy -ge 5) { break }
        if ((Get-Date) -gt $deadline) {
            throw "timeout waiting for nodes to recover"
        }
        Start-Sleep -Seconds 2
    }

    # 12. status healthy
    $statusOut = (docker compose run --rm gateway status 2>$null)
    $statusOut | ForEach-Object { Write-Host $_ }
    if ($statusOut -notmatch '"leader"') { throw "no leader in status" }
    if ($statusOut -notmatch '"mode": "rs32"') { throw "mode not rs32" }
    if ($statusOut -notmatch '"key_count": [1-9]') { throw "key_count < 1" }

    # 13. clean down
    docker compose down -v
}
finally {
    Invoke-Cleanup
}
EOF
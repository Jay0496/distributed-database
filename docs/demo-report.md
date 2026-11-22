# Demo report (commands)

Commands used for the final demo/report. Screenshots are captured from the
terminal output of these commands.

1. `docker compose up -d`
2. `docker compose ps`
3. `docker compose run --rm gateway put demo-key "hello-ta" --mode rs32`
4. `docker compose run --rm gateway get demo-key`
5. `docker compose run --rm gateway status`
6. `docker compose stop node3`
7. `docker compose run --rm gateway get demo-key`
8. `docker compose start node3`
9. `bash scripts/smoke.sh`
10. `du -sh .`

Expected status JSON example:

```json
{"leader": "node1", "degraded": [], "mode": "rs32", "key_count": 1}
```

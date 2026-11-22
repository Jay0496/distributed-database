#!/bin/sh
# Dispatch container commands: node runtime, gateway CLI, or pass-through.
if [ "$1" = "python" ] && [ "$2" = "-m" ]; then
    exec "$@"
fi
case "$1" in
    put|get|status|mode)
        exec python -m gateway.main "$@"
        ;;
esac
exec "$@"
EOF
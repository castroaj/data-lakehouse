#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ -f "$SCRIPT_DIR/.env" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$SCRIPT_DIR/.env"
    set +a
fi

JAR=$(ls -t "$SCRIPT_DIR"/build/libs/*-all.jar 2>/dev/null | head -1)

if [[ -z "$JAR" ]]; then
    echo "error: no shadow JAR found — run ./gradlew shadowJar first" >&2
    exit 1
fi

exec java -jar "$JAR" "$@"

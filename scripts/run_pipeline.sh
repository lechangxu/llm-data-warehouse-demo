#!/usr/bin/env bash
# One-shot pipeline runner: ODS -> DWD -> DWS -> ADS -> quality gates -> sft.jsonl
# Java 17+ required (PySpark 4). Prefers brew openjdk@17 if JAVA_HOME is unset.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${JAVA_HOME:-}" ] && command -v brew >/dev/null && brew --prefix openjdk@17 >/dev/null 2>&1; then
  export JAVA_HOME="$(brew --prefix openjdk@17)"
fi

PY=".venv/bin/python"
[ -x "$PY" ] || PY="python3"

exec "$PY" -m dw.pipeline

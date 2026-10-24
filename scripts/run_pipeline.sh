#!/usr/bin/env bash
# One-shot pipeline runner: ODS -> DWD -> DWS -> ADS -> quality gates -> sft.jsonl
# PySpark 4 requires Java 17 or 21. If the ambient JAVA_HOME points at an
# unsupported JVM (e.g. temurin-23), prefer brew's openjdk@17 and say so.
set -euo pipefail
cd "$(dirname "$0")/.."

java_major() { "$1/bin/java" -version 2>&1 | sed -nE 's/.*version "([0-9]+).*/\1/p' | head -1; }

need_override=1
if [ -n "${JAVA_HOME:-}" ] && [ -x "${JAVA_HOME}/bin/java" ]; then
  case "$(java_major "$JAVA_HOME")" in
    17|21) need_override=0 ;;
  esac
fi
if [ "$need_override" = "1" ] && command -v brew >/dev/null && brew --prefix openjdk@17 >/dev/null 2>&1 \
   && [ -x "$(brew --prefix openjdk@17)/bin/java" ]; then
  echo "[run_pipeline] ambient JAVA_HOME unsupported; switching to $(brew --prefix openjdk@17)" >&2
  export JAVA_HOME="$(brew --prefix openjdk@17)"
fi

PY=".venv/bin/python"
[ -x "$PY" ] || PY="python3"

exec "$PY" -m dw.pipeline

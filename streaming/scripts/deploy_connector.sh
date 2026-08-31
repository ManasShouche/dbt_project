#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then set -a; source .env; set +a; fi

PY=""
for candidate in "$ROOT/../.venv/bin/python" "${VIRTUAL_ENV:-}/bin/python" "$(command -v python3 || true)"; do
  [[ -x "$candidate" ]] || continue
  if "$candidate" -c 'import yaml' 2>/dev/null; then PY="$candidate"; break; fi
done
if [[ -z "$PY" ]]; then
  echo "No python with PyYAML found. Run 'uv sync' in the repo root first." >&2
  exit 1
fi

eval "$("$PY" scripts/resolve_profile.py "$@" | sed 's/^/export /')"

echo "profile : $RESOLVED_PROFILE"
echo "target  : $RESOLVED_TARGET"
echo "source  : $RESOLVED_PROFILES_PATH"
echo "snowflake: $SNOWFLAKE_USER@$SNOWFLAKE_ACCOUNT as $SNOWFLAKE_ROLE -> $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA"
echo

if grep -qi ENCRYPTED "$SNOWFLAKE_PRIVATE_KEY_PATH"; then
  echo "Key is encrypted; add snowflake.private.key.passphrase to the template." >&2
  exit 1
fi

mkdir -p secrets && chmod 700 secrets
KEY_PROPS="secrets/snowflake.properties"
umask 077
{
  printf 'snowflake_private_key=%s\n' \
    "$(grep -v -- '-----' "$SNOWFLAKE_PRIVATE_KEY_PATH" | tr -d '\n')"
} > "$KEY_PROPS"
echo "Wrote $KEY_PROPS (mounted read-only into the connect container)."

export KAFKA_TOPICS="${KAFKA_TOPICS:-orders,payments}"
export KAFKA_TOPIC_TABLE_MAP="${KAFKA_TOPIC_TABLE_MAP:-orders:ORDERS_STREAM,payments:PAYMENTS_STREAM}"

export KAFKA_DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-dlq.streams}"

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
RENDERED="$(mktemp)"; BODY="$(mktemp)"
trap 'rm -f "$RENDERED" "$BODY"' EXIT

"$PY" - "$ROOT/connectors/snowflake-sink.json.template" > "$RENDERED" <<'PY'
import os, re, sys
tpl = open(sys.argv[1]).read()
missing = []
def sub(m):
    k = m.group(1)
    v = os.environ.get(k)
    if v is None:
        missing.append(k)
        return m.group(0)
    return v.replace('\\', '\\\\').replace('"', '\\"')

out = re.sub(r'\$\{(\w+)\}', sub, tpl)
if missing:
    sys.exit("Unset variables: " + ", ".join(sorted(set(missing))))
sys.stdout.write(out)
PY

NAME="$("$PY" -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$RENDERED")"
"$PY" -c 'import json,sys; json.dump(json.load(open(sys.argv[1]))["config"], open(sys.argv[2],"w"))' \
  "$RENDERED" "$BODY"

echo "Deploying connector '$NAME' to $CONNECT_URL ..."
curl -sS -X PUT \
  -H 'Content-Type: application/json' \
  --data-binary "@$BODY" \
  "$CONNECT_URL/connectors/$NAME/config" >/dev/null

echo "Deployed. Status:"
sleep 3
curl -sS "$CONNECT_URL/connectors/$NAME/status" | "$PY" -m json.tool

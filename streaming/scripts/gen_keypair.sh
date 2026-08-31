#!/usr/bin/env bash
# Generate a dedicated unencrypted PKCS#8 keypair for the connector and print
# the ALTER USER statement to register it.
#
# Unencrypted on purpose: an encrypted key needs its passphrase stored in the
# same connector config as the key itself, which buys nothing. Keeping the key
# file out of git is the control that actually matters.
set -euo pipefail

KEY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/secrets"
mkdir -p "$KEY_DIR"
chmod 700 "$KEY_DIR"

PRIV="$KEY_DIR/kafka_ingest_key.p8"
PUB="$KEY_DIR/kafka_ingest_key.pub"

if [[ -f "$PRIV" ]]; then
  echo "Key already exists at $PRIV -- refusing to overwrite." >&2
  echo "Delete it first if you really want a new one." >&2
else
  openssl genrsa 2048 2>/dev/null \
    | openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt -out "$PRIV"
  chmod 600 "$PRIV"
  openssl rsa -in "$PRIV" -pubout -out "$PUB" 2>/dev/null
  echo "Generated $PRIV and $PUB"
fi

BODY="$(grep -v -- '-----' "$PUB" | tr -d '\n')"

cat <<MSG

Run this in Snowflake (step 4 of ../snowflake/01_platform.sql):

ALTER USER kafka_ingest_svc SET RSA_PUBLIC_KEY = '$BODY';

MSG

#!/usr/bin/env bash

set -euo pipefail

COUNT="${1:-200}"
TOPIC="${2:-payments}"
ORDER_BASE="${ORDER_BASE:-900000}"
ORDER_COUNT="${ORDER_COUNT:-500}"

docker compose exec -T redpanda rpk topic create "$TOPIC" -p 3 -r 1 2>/dev/null || true

docker compose exec -T redpanda rpk topic create "${KAFKA_DLQ_TOPIC:-dlq.streams}" -p 1 -r 1 2>/dev/null || true

python3 - "$COUNT" "$ORDER_BASE" "$ORDER_COUNT" <<'PY' | docker compose exec -T redpanda rpk topic produce "$TOPIC" --format '%k %v\n'
import json, random, sys
from datetime import datetime, timedelta, timezone

n, base, span = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
methods  = ['CREDIT_CARD', 'DEBIT_CARD', 'BANK_TRANSFER', 'WIRE', 'CHECK']

statuses = ['CAPTURED'] * 8 + ['PENDING', 'REFUNDED']
now = datetime.now(timezone.utc)

for i in range(n):
    payment_id = 5_000_000 + i

    order_id   = base + random.randint(0, span - 1)
    paid_at    = now - timedelta(minutes=random.randint(0, 240))

    print(payment_id, json.dumps({
        "payment_id":     payment_id,
        "order_id":       order_id,

        "amount":         round(random.uniform(500, 90000), 2),
        "payment_method": random.choice(methods),
        "payment_status": random.choice(statuses),
        "payment_ts":     paid_at.isoformat(),
        "_loaded_at":     datetime.now(timezone.utc).isoformat(),
    }))
PY

echo "Produced $COUNT payment records to '$TOPIC'."

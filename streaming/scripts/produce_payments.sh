#!/usr/bin/env bash
# Produce payment records into the `payments` topic via rpk.
# Stdlib python only -- no extra dependencies.
#
#   ./scripts/produce_payments.sh 800
#
# order_id is drawn from the range produce_orders.sh generates (900000+), so
# the relationships test on silver_payments.order_key resolves. Payments and
# orders arrive on separate topics and separate connector channels, which is
# exactly what that test proves survives.
set -euo pipefail

COUNT="${1:-200}"
TOPIC="${2:-payments}"
ORDER_BASE="${ORDER_BASE:-900000}"
ORDER_COUNT="${ORDER_COUNT:-500}"

docker compose exec -T redpanda rpk topic create "$TOPIC" -p 3 -r 1 2>/dev/null || true
# One shared DLQ for every stream -- matches KAFKA_DLQ_TOPIC in .env.example.
docker compose exec -T redpanda rpk topic create "${KAFKA_DLQ_TOPIC:-dlq.streams}" -p 1 -r 1 2>/dev/null || true

python3 - "$COUNT" "$ORDER_BASE" "$ORDER_COUNT" <<'PY' | docker compose exec -T redpanda rpk topic produce "$TOPIC" --format '%k %v\n'
import json, random, sys
from datetime import datetime, timedelta, timezone

n, base, span = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
methods  = ['CREDIT_CARD', 'DEBIT_CARD', 'BANK_TRANSFER', 'WIRE', 'CHECK']
# Weighted so CAPTURED dominates, as it would in reality. The accepted_values
# test on payment_status is what catches a new one appearing.
statuses = ['CAPTURED'] * 8 + ['PENDING', 'REFUNDED']
now = datetime.now(timezone.utc)

for i in range(n):
    payment_id = 5_000_000 + i
    # Deliberately allows more than one payment per order -- partial
    # settlements are normal, and payment_key stays the dedupe key.
    order_id   = base + random.randint(0, span - 1)
    paid_at    = now - timedelta(minutes=random.randint(0, 240))

    print(payment_id, json.dumps({
        "payment_id":     payment_id,
        "order_id":       order_id,
        # Money is NUMBER(12,2) at rest. Two decimal places here so the
        # landing table's declared scale is never the thing doing the
        # rounding.
        "amount":         round(random.uniform(500, 90000), 2),
        "payment_method": random.choice(methods),
        "payment_status": random.choice(statuses),
        "payment_ts":     paid_at.isoformat(),
        "_loaded_at":     datetime.now(timezone.utc).isoformat(),
    }))
PY

echo "Produced $COUNT payment records to '$TOPIC'."

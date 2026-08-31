#!/usr/bin/env bash

set -euo pipefail

COUNT="${1:-100}"
TOPIC="${2:-orders}"

docker compose exec -T redpanda rpk topic create "$TOPIC" -p 3 -r 1 2>/dev/null || true

docker compose exec -T redpanda rpk topic create "${KAFKA_DLQ_TOPIC:-dlq.streams}" -p 1 -r 1 2>/dev/null || true

python3 - "$COUNT" <<'PY' | docker compose exec -T redpanda rpk topic produce "$TOPIC" --format '%k %v\n'
import json, random, sys
from datetime import datetime, timedelta, timezone

n = int(sys.argv[1])
priorities = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW']
segments   = ['BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE']
modes      = ['TRUCK', 'MAIL', 'SHIP', 'AIR', 'RAIL', 'FOB', 'REG AIR']
instructs  = ['DELIVER IN PERSON', 'COLLECT COD', 'NONE', 'TAKE BACK RETURN']
base = datetime(1996, 1, 1, tzinfo=timezone.utc)

for i in range(n):
    order_key  = 900000 + i
    order_date = base + timedelta(days=random.randint(0, 2000))

    line_items = []
    for ln in range(1, random.randint(1, 7) + 1):
        quantity  = random.randint(1, 50)
        ext_price = round(quantity * random.uniform(900, 2000), 2)

        discount  = round(random.choice([0.0, 0.01, 0.02, 0.04, 0.05, 0.06, 0.08, 0.10]), 2)
        tax       = round(random.choice([0.0, 0.01, 0.02, 0.03, 0.05, 0.08]), 2)

        ship_date = order_date + timedelta(days=random.randint(1, 120))
        line_items.append({
            "line_number":    ln,
            "part_key":       random.randint(1, 200000),
            "supplier_key":   random.randint(1, 10000),
            "quantity":       quantity,
            "extended_price": ext_price,
            "discount":       discount,
            "tax":            tax,
            "return_flag":    random.choice(['N', 'R', 'A']),
            "line_status":    random.choice(['O', 'F']),
            "ship_date":      ship_date.date().isoformat(),
            "commit_date":    (ship_date + timedelta(days=random.randint(-15, 15))).date().isoformat(),
            "receipt_date":   (ship_date + timedelta(days=random.randint(1, 30))).date().isoformat(),
            "ship_instruct":  random.choice(instructs),
            "ship_mode":      random.choice(modes),
        })

    total_price = round(sum(
        round(li["extended_price"] * (1 - li["discount"]) * (1 + li["tax"]), 2)
        for li in line_items
    ), 2)

    print(order_key, json.dumps({
        "order_key":      order_key,
        "order_status":   random.choice(['O', 'F', 'P']),
        "total_price":    total_price,
        "order_date":     order_date.date().isoformat(),
        "order_priority": random.choice(priorities),
        "clerk_id":       f"Clerk#{random.randint(1, 1000):09d}",
        "ship_priority":  0,
        "order_comment":  "streamed via redpanda",
        "customer": {
            "customer_key":   random.randint(1, 150000),
            "name":           f"Customer#{random.randint(1, 150000):09d}",
            "nation_key":     random.randint(0, 24),
            "market_segment": random.choice(segments),
        },
        "line_items": line_items,

        "_loaded_at": datetime.now(timezone.utc).isoformat(),
    }))
PY

echo "Produced $COUNT nested records to '$TOPIC'."

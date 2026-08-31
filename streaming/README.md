# streaming/

Local Redpanda -> Kafka Connect -> **Snowpipe Streaming** into Snowflake.

The landing table is declared as a dbt source in `models/_sources.yml`, and
`bronze_orders` reads it. See [dbt wiring](#dbt-wiring) below.

| Piece | What it is |
|---|---|
| `docker-compose.yml` | Redpanda broker, Redpanda Console, Kafka Connect worker |
| `connect/Dockerfile` | Connect base image + the Snowflake connector JAR |
| `profiles.yml` | Named connection profiles, dbt-shaped |
| `connectors/` | Connector config template (rendered from the profile) |
| `../snowflake/` | The SQL, in run order. `01_platform` and `05_verify` are this stack's half |
| `scripts/` | Keypair generation, connector deploy, test producer |

Data lands in **`DBT_PIPE.RAW`** as a Snowflake-managed Apache Iceberg table
on the existing `EV_ICEBERG` external volume — the same schema and volume as the
hand-created Iceberg tables in `DBT_PIPE.RAW`.

## Setup

**1. Generate the keypair** (the connector supports key-pair auth only —
there is no password option):

```bash
./scripts/gen_keypair.sh
```

It prints an `ALTER USER ... SET RSA_PUBLIC_KEY` statement.

**2. Run the Snowflake setup.** Open `../snowflake/01_platform.sql`, paste
the key body into step 4, and run the whole file as `ACCOUNTADMIN`.

**3. Start:**

```bash
cp .env.example .env    # optional; every value has a default
docker compose up -d --build
```

The first build takes a few minutes while `confluent-hub` downloads the
connector. Console is at http://localhost:8080.

**4. Deploy the connector and produce data:**

```bash
./scripts/deploy_connector.sh
./scripts/produce_orders.sh 500
```

**5. Verify** with `../snowflake/05_verify.sql`. Rows should appear within
seconds.

## Notes

- **Ports.** Broker is `localhost:19092` from the host, `redpanda:9092`
  from inside compose. Connect uses the internal address.
- **The table is pre-created by you,** not by the connector. Iceberg ingest
  *requires* the target table to exist first, and pre-creating it in
  `landing/orders_stream.sql` is what pins `ICEBERG_VERSION = 3`, the base location,
  and the structured `RECORD_CONTENT` / `RECORD_METADATA` shape. The ingest
  role therefore holds `INSERT` on that one table rather than `CREATE TABLE`
  on the schema -- it cannot touch `CUSTOMER_RAW`, `NATION_RAW` or
  `REGION_RAW`. The trade is that schema evolution is now yours to do in DDL.
- **Connector v4 is streaming-only.** The installed plugin registers exactly
  one class, `SnowflakeStreamingSinkConnector`. The v2/v3 `SnowflakeSinkConnector`
  class, the `snowflake.ingestion.method` switch, and every `buffer.*` /
  `snowflake.streaming.max.client.lag` property are **gone** — v4 manages
  flushing internally. Much of the Snowflake documentation still describes
  the older connector; when the docs and the plugin disagree, ask the plugin:

  ```bash
  curl -s -X PUT -H 'Content-Type: application/json' \
    -d '{"connector.class":"com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector","topics":"orders","name":"probe"}' \
    http://localhost:8083/connector-plugins/com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector/config/validate
  ```

- **Schematization is OFF, deliberately** — even though v4 defaults it to
  `true`. Turned on, the connector infers column types from the messages and
  gives you typed columns instead of one `RECORD_CONTENT` variant, which
  looks like a free win. It is not, for JSON: **JSON numbers carry no scale**,
  so a monetary field lands as `NUMBER(38,0)` and every value is silently
  rounded to a whole unit. Measured here, `total_price` landed as `56375`
  where the true value was `56374.76` — enough to break reconciliation against
  the line items on 248 of 300 orders, while the same numbers nested inside
  the semi-structured payload kept full precision.

  That failure mode is now structurally impossible regardless: the Iceberg
  landing table declares `total_price NUMBER(12,2)` inside a typed
  `RECORD_CONTENT OBJECT(...)`, so the scale is fixed at rest rather than
  inferred at write time.

  With it off, the landing table is exactly what arrived and `bronze_orders`
  casts explicitly (`::number(38,2)` for money), so every type decision is in
  version control rather than inferred at runtime from whichever record
  happened to arrive first.

  Schematization is worth revisiting with **Avro or Protobuf** via the Schema
  Registry on `localhost:18081` — those carry real types including decimal
  scale, so the inference has something to work from.
- **Exactly-once** is inherent to the streaming path — each Kafka partition
  maps 1:1 to a Snowflake channel, and the offset token commits atomically
  with the rows. Query 3 in `05_verify.sql` is what proves it held.
- **Targeting Iceberg** is `snowflake.autocreate.table.type` (`snowflake` |
  `iceberg`) plus `snowflake.iceberg.create.table.options` in v4, not the
  older `snowflake.streaming.iceberg.enabled` flag.
- **Images are unpinned** (`:latest`) so this keeps working. Pin them once
  you have a combination you like.
- **Cost:** Snowpipe Streaming is serverless and bills independently of
  `wh_transform`. Ingest continues as long as the connector runs — stop it
  with `docker compose down` when you are done.

## Iceberg ingestion config

The connector config is JSON and cannot carry comments, so the reasoning for
the Iceberg-specific settings lives here.

- **`snowflake.enable.schematization: false`** — required. The landing table
  declares `RECORD_CONTENT` as a structured `OBJECT(...)` covering the whole
  nested payload. Schematization would instead try to derive flat columns,
  which is both unnecessary and the source of the rounding bug above.

- **`snowflake.autocreate.table.type: iceberg`** — governs what the connector
  would create if the target table were missing. It is belt-and-braces here,
  because `landing/orders_stream.sql` pre-creates the table: Iceberg ingestion
  *requires* the table to exist first, and pre-creating is what lets us pin
  `ICEBERG_VERSION = 3` and the `raw/orders_stream/` base location rather
  than accepting whatever the connector picks.

- **No `snowflake.streaming.iceberg.enabled`, no
  `snowflake.streaming.enable.single.buffer`.** Both are v3 properties. v4 is
  a ground-up rewrite on the high-performance Snowpipe Streaming architecture
  and auto-detects Iceberg targets — the release notes state Iceberg ingestion
  needs "no special configuration". Setting the v3 flags here is cargo-cult.

- **`VARIANT` is not an option.** For Iceberg targets the connector rejects it
  outright: "the semi-structured VARIANT type isn't supported". A structured
  `OBJECT` or `MAP` is the only way to land nested JSON.

- **JSON converter, `schemas.enable: false`.** Fine for a fixed payload whose
  fields all appear in the declared `OBJECT`. The limitation to know: schema
  *evolution* on Iceberg is only fully supported for Avro/Protobuf. Adding a
  field to the producer without adding it to the table DDL is the failure this
  setup is exposed to — watch `dlq.streams` after any payload change.

## Connection profiles

Kafka Connect has **no native profiles concept** — a connector config is a
flat JSON map POSTed to the REST API, and that is the only thing the worker
understands. So `profiles.yml` here is resolved at *deploy* time by
`scripts/resolve_profile.py` and rendered into that flat map.

The shape is dbt's on purpose:

```yaml
tpch_stream:
  target: dev
  outputs:
    dev:
      account: ${SNOWFLAKE_ACCOUNT}
      user: kafka_ingest_svc
      role: kafka_ingest
      database: DBT_PIPE
      schema: RAW
      private_key_path: ./secrets/kafka_ingest_key.p8
    prod:
      ...
```

Switching is a flag, never an edit to a connector config:

```bash
./scripts/deploy_connector.sh                              # profile default target
./scripts/deploy_connector.sh --target prod
./scripts/deploy_connector.sh --profile tpch_stream --target dev
```

Set `CONNECTOR_PROFILE` in `.env` to change the default, which is the
analogue of the `profile:` key in `dbt_project.yml`.

**It is independent of the project.** The resolver searches
`./profiles.yml`, then `~/.dbt/profiles.yml`, and `CONNECTOR_PROFILES_PATH`
overrides both. The key names it reads are dbt's own, so your existing dbt
profile resolves with no translation:

```bash
CONNECTOR_PROFILES_PATH=~/.dbt/profiles.yml \
  ./scripts/deploy_connector.sh --profile tpch_pipeline --target dev
```

That works, but it connects as `transformer` into `dbt_manas` — the role and
schema dbt builds with. For ingest you want the narrow `kafka_ingest` role,
which is why `tpch_stream` exists as its own profile.

Only connection details belong here. Topics, task counts and buffer tuning
stay in `connectors/snowflake-sink.json.template` — the same split dbt draws
between `profiles.yml` and `dbt_project.yml`.

### Where the private key actually goes

Not into the connector config. Connect persists connector configs to the
`_connect_configs` topic and echoes them back over the REST API, so an
inlined key would sit in plaintext in both.

Instead the worker runs Kafka Connect's `FileConfigProvider`, and the
template references the key indirectly:

```json
"snowflake.private.key": "${file:/secrets/snowflake.properties:snowflake_private_key}"
```

`deploy_connector.sh` writes that properties file from the profile's
`private_key_path` into `secrets/`, which is mounted read-only into the
container and gitignored. The worker resolves the reference at task startup.
So `profiles.yml` holds a path, `secrets/` holds the key, and the connector
config holds neither.

## Troubleshooting

### "It's producing but nothing lands in Snowflake"

Check the connector **state** first — a sink that fails to start consumes
nothing at all, silently:

```bash
curl -s localhost:8083/connectors/snowpipe-orders/status | python3 -m json.tool
```

`FAILED` at the connector level (no tasks listed) means it died in `start()`,
before touching Kafka or Snowflake. The reason is in `.connector.trace`.

The one that will bite you on a fresh install: v4 ships with
`snowflake.streaming.validate.compatibility.with.classic=true`, a **migration
gate** that then demands four properties be *explicitly* set
(`snowflake.streaming.classic.offset.migration`, `snowflake.validation`, and
two `snowflake.compatibility.*` flags). It exists to protect people upgrading
from connector v3. On a greenfield deployment none of it applies, so the
template sets:

```json
"snowflake.streaming.validate.compatibility.with.classic": "false"
```

Note that `/connector-plugins/.../config/validate` returns **error_count: 0**
for a config that trips this gate — the REST validator does not run the
connector's own `start()` validation. A clean validate is not proof the
connector will start.

### Changing the payload shape

Schematization never removes a column, and offset tokens survive in the
channel. If the producer's payload changes shape, drop the landing table and
delete the connector — the RESET section at the bottom of
`../snowflake/01_platform.sql` has the statements. Note the table is owned
by `kafka_ingest`, not by your dbt role, so the drop needs `ACCOUNTADMIN` or
a connection as the service user itself.

### Kafka consumer-group lag is NOT the health signal

```
$ rpk group describe connect-snowpipe-orders
TOTAL-LAG  525
orders  0  CURRENT-OFFSET  -   LAG 168
```

This is expected even when everything has landed. The connector tracks
position as an **offset token on the Snowflake channel**, not as a Kafka
consumer-group commit — that is exactly what makes exactly-once work across
restarts. So `CURRENT-OFFSET` stays `-` and lag reads as the full topic size
forever.

Look at the worker log instead, where the truth is:

```
Channel SNOWPIPE_ORDERS_..._orders_1 has SSv2 offset token 183
Initializing offsetPersistedInSnowflake=[183]
Rewinding offsets for skipped partitions: {orders-1=184}
Seeking to offset 184 for partition orders-1
```

Token 183 on a partition whose high-watermark is 184 means fully caught up:
offsets 0-183 are committed in Snowflake, and it resumes at 184.

Or just count rows in Snowflake, which settles it:

```sql
select count(*) from dbt_pipe.raw.orders_stream;
```

## dbt wiring

`DBT_PIPE.RAW.ORDERS_STREAM` is declared in `models/_sources.yml`, alongside
the batch dimension tables -- there is one raw landing zone and one source.

`bronze_orders` appends the payload with its delivery metadata lifted to
scalars, still nested. Expanding it is silver's job: `silver_orders` reads the
order header, `silver_line_items` explodes the `line_items` array. There is no
`bronze_line_items` -- an earlier layout had one, and flattening in bronze
meant the header and its lines could be deduplicated to different deliveries.

The dedupe runs in silver and orders on the arrival clock, with the Kafka
offset as the tiebreaker:

```sql
qualify row_number() over (
    partition by order_key
    order by ingested_at  desc nulls last,
             kafka_offset desc nulls last
) = 1
```

Both parts matter. `ingested_at` is `SnowflakeConnectorPushTime`, which is
stamped by the connector and so always increases on re-delivery -- unlike
`_loaded_at`, which the producer sets and which therefore moves backwards on a
replay. The offset breaks ties: streamed rows arrive in dense clusters where
many share a timestamp, exactly the rows the ordering is supposed to resolve.
Offsets are monotonic per partition and totally ordered.

`NULLS LAST` is load-bearing. Snowflake sorts NULLs first on `DESC`, so
without it the 500 legacy rows with no metadata would outrank properly
stamped ones and win the dedupe.

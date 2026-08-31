# Adding a stream

Two claims, and `payments` is the worked example that tests both:

- **100 topics, one connector.** `snowflake.schema.name` is a single value, so
  every topic lands in one schema and each gets its own table via
  `topic2table.map`. Adding a stream is an edit to a comma-separated list, not
  a second worker.
- **100 streams, 100 config rows** -- not 100 SQL implementations.

Adding one is three steps. None involves writing transformation logic.

`payments` cost exactly: one landing table from the template, one row in
`stream_config.csv`, seven in `stream_column_config.csv`, a copied
`bronze_payments.sql`, and a `silver_payments.sql` whose entire body is
`{{ build_silver_stream('payments') }}`. No new grants -- the future grants on
`RAW_STREAMS` covered the table the moment it was created.

---

## 0. Once per account

Run [`snowflake/01_platform.sql`](../snowflake/01_platform.sql)
as `ACCOUNTADMIN`. It creates the `DBT_PIPE.RAW_STREAMS` schema and the
**future grants** that make every later landing table readable and writable
without touching grants again.

Landing tables live in `RAW_STREAMS`, not `RAW`, specifically so those grants
can be broad: a future `INSERT` on `RAW` would hand the connector write access
to `CUSTOMER_RAW`, `NATION_RAW` and `REGION_RAW` as well.

## 1. Land it

Copy [`snowflake/02_landing/_template.sql`](../snowflake/02_landing/_template.sql),
replace `<STREAM>`, fill in the payload shape, run as `ACCOUNTADMIN`. No
grants needed — step 0 covered them.

Then add the topic to the **existing** connector — do not deploy another one.
Both lists live in `streaming/.env`:

```bash
KAFKA_TOPICS=orders,payments,users
KAFKA_TOPIC_TABLE_MAP=orders:ORDERS_STREAM,payments:PAYMENTS_STREAM,users:USERS_STREAM
```

The defaults in `deploy_connector.sh` are already `orders,payments`; `.env`
only needs editing for a third.

```bash
cd streaming && ./scripts/deploy_connector.sh
```

The two lists must agree. A topic in `KAFKA_TOPICS` with no mapping does not
error — the connector derives a table name from the topic and lands rows
somewhere nobody is reading.

### How Snowflake handles many streams on one connector

- **One connector, one schema.** `snowflake.schema.name` is a single value, so
  every topic lands in the same schema. That is why landing tables belong in
  `RAW_STREAMS` — one schema, one set of future grants, every stream covered.
- **One table per topic**, via `topic2table.map`. Heterogeneous payloads should
  not share a table; the structured `OBJECT` in each landing table's DDL is
  what pins types, and a shared table would have to give that up.
- **One channel per topic-partition.** Snowpipe Streaming commits the Kafka
  offset token atomically with the rows on each channel, so exactly-once holds
  **per stream independently**. One topic replaying or falling behind does not
  affect the others.
- **Ingest is serverless.** It does not consume `wh_transform`, so adding
  streams does not contend with dbt for warehouse capacity.
- **`tasks.max` is the one thing to revisit.** It is `2` today, sized for one
  topic. Tasks are distributed across topic-partitions, so with many topics
  raise it or a single task serialises several streams.

## 2. Configure it

One row in [`seeds/config/stream_config.csv`](../seeds/config/stream_config.csv)
-- this is the real `payments` row:

```csv
payments,payments,payments_stream,bronze_payments,silver_payments,record_content,payment_key,ingested_at,payment_ts,incremental,15,true
```

`watermark_field` is `ingested_at`, never `payment_ts`. `sla_minutes` is 15
against orders' 60, and `stream_audit` holds each stream to its own budget.

One row per field in [`seeds/config/stream_column_config.csv`](../seeds/config/stream_column_config.csv):

```csv
payments,payment_id,payment_key,number,1
payments,order_id,order_key,number,2
payments,amount,amount,"number(38,2)",3
payments,payment_method,payment_method,string,4
payments,payment_status,payment_status,string,5
payments,payment_ts,paid_at,timestamp_ntz,6
payments,_loaded_at,_loaded_at,timestamp_ntz,7
```

`ordinal` is the built table's column order, and `amount` carries scale --
money must, or the cast rounds it.

Dots traverse nesting: `customer.customer_key` reads one level into the
payload object.

## 3. Point a model at it

A bronze model (append-only lineage extraction, copy `bronze_orders`), and a
silver model that is essentially three lines:

```sql
{{ config(materialized='incremental', unique_key='payment_key',
          incremental_strategy='merge') }}

-- depends_on: {{ ref('stream_config') }}
-- depends_on: {{ ref('stream_column_config') }}
-- depends_on: {{ ref('bronze_payments') }}

{{ build_silver_stream('payments') }}
```

That is the whole model. The 7-column typed extract, the lookback filter and
the dedupe are all generated -- see
`target/compiled/.../silver_payments.sql`.

Add one `-- depends_on:` line for the new silver model to
[`models/audit/stream_audit.sql`](../models/audit/stream_audit.sql) so it
appears in monitoring.

Then `dbt build`.

## 4. Wire it into the trigger

The build is event-driven, so a new landing table needs an arrival stream and
a place in the root task's condition. Both live in
[`snowflake/04_pipeline.sql`](../snowflake/04_pipeline.sql):

```sql
CREATE STREAM IF NOT EXISTS dbt_pipe.raw_streams.users_arrivals
    ON TABLE dbt_pipe.raw_streams.users_stream
    SHOW_INITIAL_ROWS = FALSE;
```

Add `OR SYSTEM$STREAM_HAS_DATA('dbt_pipe.raw_streams.users_arrivals')` to the
`WHEN` clause, and a `SELECT 'users', COUNT(*) FROM ...users_arrivals` branch
to the task body. That second edit is not optional: a stream only advances
when a DML statement consumes it, and an arrival stream nothing consumes will
hold the condition true forever and keep the warehouse awake.

---

## Why the dependencies are written out

dbt builds its DAG at **parse** time, before any warehouse connection exists,
so the config tables cannot be read to discover what a model depends on. The
macro returns a placeholder during parse and the real SQL during execution;
the `-- depends_on:` lines are what tell dbt the true shape of the graph.

This is also why every stream model must depend on the config seeds: they have
to be loaded **before** the model compiles, or `run_query` reads a table that
is not there.

## What the framework does not do

`build_silver_stream` handles the common shape — a flat extract from one
payload object, one row in, one row out.

A payload carrying an **array to explode** is a different grain and needs its
own model. [`silver_line_items`](../models/silver/silver_line_items.sql) stays
hand-written for exactly this reason: it uses `LATERAL FLATTEN` and
`delete+insert` rather than `merge`, because a re-delivered order can carry
*fewer* lines than the stored copy and merge would leave the dropped ones as
orphans.

Forcing one-to-many into the macro would make it harder to read for every
stream, to serve a minority of them. Better to have one clear macro and a
small number of deliberate exceptions.

## Choosing a watermark

`watermark_field` must be **platform-assigned**, which in practice means
`ingested_at` (`SnowflakeConnectorPushTime`).

Not `_loaded_at`, and not `CreateTime`: both are set by the producer, so on a
replay of old messages they carry their original values, land below the
high-water mark, and are skipped **without erroring**. A watermark that can
move backwards drops data you never notice is missing.

Every incremental filter uses `watermark > max(watermark) - lookback`, never a
bare `>`. Out-of-order arrival is normal — a partition retry, a connector
restart, a rebalance — and the overlap costs a re-read that MERGE turns into a
no-op. `late_arrival_hours` in `dbt_project.yml` sets the window.

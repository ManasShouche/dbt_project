# tpch-clean

TPC-H medallion pipeline on Snowflake. Events land in Apache Iceberg through
Snowpipe Streaming; dbt builds bronze -> silver -> gold on top, and it builds
**when data arrives**, not on a schedule.

```text
Redpanda -> Kafka Connect -> Snowpipe Streaming
  ONE connector, N topics, one schema (snowflake.schema.name is a single value)

  DBT_PIPE.RAW_STREAMS  (Iceberg v3 on EV_ICEBERG)
    ORDERS_STREAM   -> bronze_orders   -> silver_orders      -> fct_orders
                                       -> silver_line_items  -^
    PAYMENTS_STREAM -> bronze_payments -> silver_payments

  DBT_PIPE.RAW          (batch, connector has no access)
    customer_raw / nation_raw / region_raw -> silver_* -> dim_customers
```

Landing is the core of it. Everything downstream is a consequence: the raw
layer is Iceberg because the connector writes it directly, bronze is
append-only because delivery is at-least-once, and the whole medallion
rebuilds because rows showed up.

## The event-driven build

A Snowflake stream on each landing table detects arrivals. A root task
consumes those streams (which is what advances them) and a child task runs
the deployed dbt project:

```text
rows land in RAW_STREAMS
      -> ORDERS_ARRIVALS / PAYMENTS_ARRIVALS have data
      -> t_detect_arrivals fires, logs to CONTROL.INGEST_LOG
      -> t_run_dbt runs EXECUTE DBT PROJECT ... 'build --target dev'
      -> bronze -> silver -> gold, tests included
```

Idle costs nothing: the task's `WHEN SYSTEM$STREAM_HAS_DATA(...)` condition is
evaluated without a warehouse. See
[`snowflake/README.md`](snowflake/README.md) for why it is two tasks rather
than one, which is the part that is easy to get wrong.

## Running it

```bash
cp snowflake/.env.example snowflake/.env      # bucket, role ARN, external id
./snowflake/deploy.sh --verify                # platform, landing, dims, dbt project, tasks

cd streaming
cp .env.example .env                          # SNOWFLAKE_ACCOUNT
docker compose up -d --build
./scripts/deploy_connector.sh
./scripts/produce_orders.sh 500
./scripts/produce_payments.sh 200
```

Rows land within seconds, the task graph picks them up within a minute, and
the medallion rebuilds. To start from empty without changing any table shape:

```bash
./snowflake/deploy.sh --reset --verify
```

For local dbt runs against the same account:

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # then fill in the env vars
dbt build
```

Targets are `dev` and `prod`; `dev` is the default. Models read
`target.database` rather than hardcoding a database, so the same checkout runs
against either without edits.

## Metadata-driven streams

Silver stream models are generated: which bronze model to read, which payload
fields to extract, how to cast them and what to deduplicate on all come from
two config seeds rather than from SQL. Adding a stream is a config row, not an
implementation — see [`docs/streams.md`](docs/streams.md).

Two streams share one connector. Adding the second needed no new connector, no
new grants and no transformation SQL — a landing table, two config row sets
and a three-line model.

## Storage: Snowflake catalog, open files

Everything operational happens inside Snowflake. Kafka Connect hands records
to **Snowpipe Streaming**, which lands them directly in Iceberg tables; dbt
transforms them on `wh_transform`. There are no AWS services in the data path
— no Kinesis, no Firehose, no Glue, no Athena, no Lambda.

Snowflake Iceberg tables require an `EXTERNAL VOLUME`, so the files themselves
live in cloud object storage you own rather than in Snowflake's internal
storage:

```text
Redpanda -> Kafka Connect -> Snowpipe Streaming
  -> DBT_PIPE.RAW_STREAMS.ORDERS_STREAM          catalog + compute: Snowflake
  -> s3://<bucket>/iceberg/raw_streams/...       Parquet + metadata: your bucket
```

Check it for any table:

```sql
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('DBT_PIPE.RAW_STREAMS.ORDERS_STREAM');
```

That split is what every Iceberg constraint in this project is paying for:
`ICEBERG_VERSION = 3` for the nested payload, pre-created tables, no
`VARIANT`, `RECORD_METADATA` declared by hand. In exchange the raw layer is an
open format in storage you control.

The volume is `EV_ICEBERG`, defined in
[`snowflake/01_platform.sql`](snowflake/01_platform.sql).
`SYSTEM$VERIFY_EXTERNAL_VOLUME('EV_ICEBERG')` tests read, write, list and
delete against it.

## A green build does not mean the pipeline is live

`dim_customers` and the three `silver_*` dimension models are dynamic tables,
and Snowflake suspends a dynamic table when a table it reads is dropped. It
does not resume when that table comes back.

dbt repopulates a suspended dynamic table quite happily, so the build stays
green — correct rows, passing tests — while scheduled refresh is dead and the
tables go stale from that moment on. They once sat suspended for two days
across builds that all reported PASS.

`dbt build` will never tell you. `snowflake/05_verify.sql` checks it directly;
every row must read `ACTIVE`:

```sql
SHOW DYNAMIC TABLES IN DATABASE dbt_pipe;
SELECT "name", "scheduling_state", "target_lag"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

## Deploying to dbt Projects on Snowflake

`deploy.sh` does this for you. The constraints behind it:

- **The dbt version is pinned to 1.11.11, and that is not optional.**
  Snowflake offers 1.11.11 / 1.10.15 / 1.9.4 and `EXECUTE DBT PROJECT`
  defaults to 1.9.4, which cannot compile this project:

  ```text
  Compilation Error in test relationships_silver_orders_...
    macro 'dbt_macro__test_relationships' takes no keyword argument 'arguments'
  ```

  The `arguments:` form used throughout the schema YAML arrived in dbt 1.10.
  `04_pipeline.sql` also sets the account default so it cannot be forgotten.

- **The profile ships with the project.**
  [`dbt_projects_profiles.yml`](dbt_projects_profiles.yml) is committed for
  exactly this: Snowflake reads the profile from inside the project folder,
  there is no `~/.dbt` to fall back on, and `--profiles-dir` is not supported
  there. It holds no credentials — the project runs under the invoking
  session, so `account` and `user` are ignored. `profiles.yml` stays
  gitignored and is the local-only file.

- **The project has no package dependencies, on purpose.** Snowflake runs
  `dbt deps` at compile time, so a Hub package needs an external access
  integration, and those are unavailable on trial accounts entirely
  (`509009 ... External access is not supported for trial accounts`).

- **Never deploy from the working directory.** `--source` uploads everything
  it finds, including `streaming/secrets/` and a 291MB `.venv`, against a
  20,000-file limit. `deploy.sh` stages a clean copy first.

- **`dbt clean` whenever you switch dbt versions.** Snapshot artifacts written
  by one version break the other in both directions — 1.12 wants a *directory*
  at `target/run/tpch_clean/snapshots/snap_customers.sql` where 1.11 wants a
  *file*.

- **`dbt source freshness` and `dbt debug` are not supported commands** there.
  Both stay runnable locally.

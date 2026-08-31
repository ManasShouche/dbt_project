# tpch-clean

TPC-H medallion pipeline on Snowflake: streamed order events land in an Apache
Iceberg table, dbt builds bronze -> silver -> gold on top.

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

Two streams share one connector. Adding the second needed no new connector,
no new grants and no transformation SQL -- a landing table, two config row
sets and a three-line model. See [`docs/streams.md`](docs/streams.md).

The streaming half is set up in [`streaming/`](streaming/).

Silver stream models are **metadata-driven**: which bronze model to read, which
payload fields to extract, how to cast them and what to deduplicate on all come
from two config tables rather than from SQL. Adding a stream is a config row,
not an implementation — see [`docs/streams.md`](docs/streams.md).

## Storage: Snowflake catalog, open files

Everything operational happens inside Snowflake. Kafka Connect hands records
to **Snowpipe Streaming**, which lands them directly in Iceberg tables; dbt
transforms them on `wh_transform`. There are no AWS services in the data path
-- no Kinesis, no Firehose, no Glue, no Athena, no Lambda.

The raw layer is Apache Iceberg, and Snowflake Iceberg tables require an
`EXTERNAL VOLUME`, so the files themselves live in cloud object storage you
own rather than in Snowflake's internal storage:

```text
Redpanda -> Kafka Connect -> Snowpipe Streaming
  -> DBT_PIPE.RAW_STREAMS.ORDERS_STREAM          catalog + compute: Snowflake
  -> s3://<bucket>/iceberg/raw_streams/...       Parquet + metadata: your bucket
```

Check it for any table:

```sql
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('DBT_PIPE.RAW_STREAMS.ORDERS_STREAM');
-- {"metadataLocation":"s3://<bucket>/iceberg/raw_streams/orders_stream.../metadata/00001-....metadata.json"}
```

That split is the whole point, and it is what every Iceberg constraint in this
project is paying for: `ICEBERG_VERSION = 3` for the nested payload,
pre-created tables, no `VARIANT`, `RECORD_METADATA` declared by hand. In
exchange the raw layer is an open format in storage you control, readable by
anything that speaks Iceberg rather than only by Snowflake.

The volume is `EV_ICEBERG`, defined in
[`snowflake/01_platform.sql`](snowflake/01_platform.sql) along with the bucket and
IAM role it expects. `SYSTEM$VERIFY_EXTERNAL_VOLUME('EV_ICEBERG')` tests
read, write, list and delete against it.

Nothing outside Snowflake reads these files today -- that portability is
available, not yet demonstrated.

## Running locally

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # then fill in the env vars
dbt deps
dbt build
```

Targets are `dev` and `prod`; `dev` is the default. Models read
`target.database` rather than hardcoding a database, so the same checkout runs
against either without edits.

A green build is `PASS=95 WARN=5 ERROR=0` across 100 nodes. Four warnings are
the same known gap -- 500 rows landed before `RECORD_METADATA` existed and
carry NULL lineage permanently, see
[`models/bronze/_bronze__models.yml`](models/bronze/_bronze__models.yml). The
fifth is `stream_audit`: `payments` has no rows yet, so its SLA is NULL rather
than false. It clears as soon as the topic carries data.

### A green build does not mean the pipeline is live

`dim_customers` and the three `silver_*` dimension models are dynamic tables,
and Snowflake suspends a dynamic table when a table it reads is dropped. It
does not resume when that table comes back.

dbt repopulates a suspended dynamic table quite happily, so the build stays
green -- correct rows, passing tests -- while scheduled refresh is dead and the
tables go stale from that moment on. This is not hypothetical: they sat
suspended from 2026-08-25 with zero refreshes for two days, across builds that
all reported PASS.

`dbt build` will never tell you. Check the state directly:

```sql
SHOW DYNAMIC TABLES IN DATABASE dbt_pipe;
SELECT "name", "scheduling_state", "target_lag"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
```

Every row must read `ACTIVE`. If any reads `SUSPENDED`, resume upstream first
-- `snowflake/03_raw_data.sql` ends with the statements in order.

### Raw dependencies

`silver_customers`, `silver_nations` and `silver_regions` read
`CUSTOMER_RAW` / `NATION_RAW` / `REGION_RAW` in `DBT_PIPE.RAW`. Those are not
created by dbt. If they are missing the build fails with
`Object 'DBT_PIPE.RAW.NATION_RAW' does not exist or not authorized` and the
whole customer chain skips -- recreate them with

```bash
snow sql -c tpch -f snowflake/03_raw_data.sql
```

The INSERTs in that script are not idempotent. Confirm the tables are absent
before running it.

## Deploying and running via Snowflake CLI

Deploy from a clean copy of the committed tree, never from the working
directory. `--source` uploads everything it finds, and `.venv/` alone is 291MB
against a 388KB project:

```bash
rm -rf /tmp/deploy && mkdir -p /tmp/deploy
git archive HEAD | tar -x -C /tmp/deploy && cd /tmp/deploy

# No --dbt-version: snowflake/04_dbt_project.sql pins the account default
# to 1.11.11. No --external-access-integration: there are no packages to fetch.
snow dbt deploy dbt_pipe.control.tpch_clean -c tpch \
    --source . --profiles-dir . \
    --default-target dev
```

Then run it. **`-c` must come before the project name.** Everything after the
dbt subcommand is forwarded verbatim to dbt, so a trailing `-c tpch` gets eaten
by dbt and `snow` fails with `Connection default is not configured` -- an error
that points at your config file rather than at argument order:

```bash
snow dbt execute -c tpch dbt_pipe.control.tpch_clean build
snow dbt execute -c tpch dbt_pipe.control.tpch_clean test
snow dbt execute -c tpch --run-async dbt_pipe.control.tpch_clean build

snow dbt list -c tpch
snow dbt describe dbt_pipe.control.tpch_clean -c tpch
```

`--source .` picks up `dbt_project.yml`, `--profiles-dir .` picks up
`dbt_projects_profiles.yml`, which takes precedence over `profiles.yml` and is
staged under its own name. Avoid `--force` -- it recreates the object and drops
every version and run-history entry.

The SQL equivalent, if you would rather drive it from a task:

```sql
EXECUTE DBT PROJECT dbt_pipe.control.tpch_clean
  ARGS = 'build --target dev';
```

Verified end to end on 2026-08-27: deployed as `DBT_PIPE.CONTROL.TPCH_CLEAN`
(VERSION$1, dbt 1.11.11, dbt-snowflake 1.11.5), and `build` returned
`PASS=59 WARN=4 ERROR=0`, matching the local run at that commit (63 nodes).
The project has grown to 100 nodes since -- the metadata-driven stream
framework and the `payments` stream -- so `VERSION$1` is stale. Redeploy
before trusting that number again.


## Deploying to dbt Projects on Snowflake

The project is compatible as-is, but note the following.

- **Pin the dbt version to 1.11.11. This is not optional.** Snowflake offers
  1.11.11 / 1.10.15 / 1.9.4, and `EXECUTE DBT PROJECT` defaults to **1.9.4**
  when nothing is pinned. This project does not build on 1.9.4:

  ```text
  Compilation Error in test relationships_silver_orders_...
    macro 'dbt_macro__test_relationships' takes no keyword argument 'arguments'
  ```

  The `arguments:` form used throughout the schema YAML arrived in dbt 1.10.
  `snowflake/04_dbt_project.sql` sets the account default to 1.11.11 so
  this cannot be forgotten.

  Verified green on 1.11.11 (dbt-snowflake 1.11.6) and locally on 1.12.2,
  which Snowflake does not offer.

- **The profile ships with the project.**
  [`dbt_projects_profiles.yml`](dbt_projects_profiles.yml) is committed for
  exactly this: Snowflake reads the profile from inside the project folder,
  there is no `~/.dbt` to fall back on, and `--profiles-dir` is not a
  supported flag. It holds no credentials -- the project runs under the
  invoking session, so `account` and `user` are ignored. `profiles.yml` stays
  gitignored and is the local-only file; when both are present Snowflake uses
  `dbt_projects_profiles.yml`.

- **The project has no package dependencies, on purpose.** There is no
  `packages.yml`, so `dbt deps` has nothing to fetch and no egress is needed.
  That is a deployment constraint, not a preference: Snowflake runs `dbt deps`
  at compile time, a Hub package therefore needs an external access
  integration, and those are unavailable on trial accounts entirely
  (`509009 ... External access is not supported for trial accounts`). The cost
  was minimal -- codegen was interactive-only and referenced by nothing, and
  the single dbt_utils test is now
  `tests/assert_line_items_unique_per_order.sql`. See
  `snowflake/04_dbt_project.sql` for how to put the dependency back if the
  account is upgraded.

- **Do not upload `target/`, and `dbt clean` whenever you switch dbt
  versions.** Beyond the 20,000-file limit, snapshot artifacts written by one
  version break the other, in both directions. At
  `target/run/tpch_clean/snapshots/snap_customers.sql`, 1.12 wants a
  *directory* containing `snap_customers.sql` where 1.11 wants a *file*, so
  the snapshot fails with whichever the leftover happens to contradict:

  ```text
  1.12 artifacts, then 1.11:  [Errno 21] Is a directory
  1.11 artifacts, then 1.12:  [Errno 20] Not a directory
  ```

  Nothing is wrong with the project when this fires -- `dbt clean` and
  rebuild.

- **`dbt source freshness` and `dbt debug` are not supported commands** there.
  The sources in [`models/_sources.yml`](models/_sources.yml) configure
  freshness; it stays runnable locally.

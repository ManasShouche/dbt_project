# snowflake/

Account-side SQL. dbt owns the `DBT_MANAS_*` schemas; these files own
everything dbt and the connector assume already exists.

The numbering is the pipeline: a platform to land into, the Snowpipe
Streaming landing tables, the batch dimensions beside them, then the
medallion build that fires when data arrives.

| File | Purpose |
|---|---|
| `01_platform.sql` | Database, schemas, warehouse, roles, external volume, future grants, ingest service user |
| `02_landing/<topic>_stream.sql` | One Snowpipe Streaming landing table per Kafka topic |
| `03_dimensions.sql` | Batch dimension tables, loaded from `snowflake_sample_data` |
| `04_pipeline.sql` | dbt version pin, the deployed project's task graph, event-driven trigger |
| `05_verify.sql` | Read-only health checks, landing through gold |
| `99_reset.sql` | Wipe data and dbt-built schemas, keeping every table shape |

## Running it

Everything account-specific lives in `snowflake/.env`, which is gitignored:

```bash
cp snowflake/.env.example snowflake/.env   # then fill in bucket, role ARN, external id
./snowflake/deploy.sh                      # 01 -> 02 -> 03 -> dbt project -> 04
./snowflake/deploy.sh --verify             # ...and run 05 at the end
./snowflake/deploy.sh --reset --verify     # wipe first, then rebuild from empty
```

`deploy.sh` renders the three storage values into `01_platform.sql` with
`snow sql -D`, so no account or bucket identifier is committed. It stages the
dbt project into a temp directory before `snow dbt deploy`, because `--source`
uploads every file it finds and the working tree holds `streaming/secrets/`
and a `.venv`.

Order is load-bearing. `01_platform.sql` must run before anything in
`02_landing/`: a future grant applies only to objects created after it, so a
landing table created first comes back with no grants and the connector fails
on a table that looks healthy. `04_pipeline.sql` must run after the dbt
project object exists, because its task references it by name.

## The event-driven build

`04_pipeline.sql` builds the medallion when data lands, not on a wall clock.

A triggered task fires while its stream has data, and a stream only advances
when a DML statement consumes it. `EXECUTE DBT PROJECT` reads the base tables
and never the stream, so a task that only ran dbt would leave the stream
permanently unconsumed and fire forever — a warehouse that never suspends.

Hence two tasks:

```text
t_detect_arrivals   root. SCHEDULE '1 MINUTE' + WHEN SYSTEM$STREAM_HAS_DATA(...)
                    consumes both arrival streams into CONTROL.INGEST_LOG,
                    which is what advances them
      |
      v
t_run_dbt           child. EXECUTE DBT PROJECT ... ARGS = 'build --target dev'
```

`SCHEDULE` plus `WHEN`, rather than a bare triggered task. The condition is
evaluated without a warehouse and costs nothing when false, so this is still
event-driven — it builds only when rows actually arrived. The schedule is a
rate floor: a full build takes ~18s and a pure triggered task polls every
~12s, so a continuously-fed topic would rebuild back to back and the warehouse
would never suspend. For true trigger-on-arrival (~12s latency, only sane for
bursty sources), drop the `SCHEDULE` line.

Both tasks are resumed by the script. To disarm the pipeline without tearing
anything down:

```sql
ALTER TASK dbt_pipe.control.t_detect_arrivals SUSPEND;
```

## Adding a topic

One connector serves every topic and `snowflake.schema.name` is a single
value, so all topics land in `RAW_STREAMS`, each routed to its own table by
`topic2table.map`. Never deploy a second connector.

1. Copy `02_landing/_template.sql` to `02_landing/<topic>_stream.sql`, declare
   the payload shape, rerun `deploy.sh`. No grants needed.
2. Add the topic to `KAFKA_TOPICS` and `KAFKA_TOPIC_TABLE_MAP`, redeploy the
   connector.
3. Add an arrival stream and extend the `WHEN` condition in `04_pipeline.sql`.
4. Config rows and a three-line silver model — see
   [`../docs/streams.md`](../docs/streams.md).

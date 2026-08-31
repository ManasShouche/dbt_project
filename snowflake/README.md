# snowflake/

Account-side SQL. dbt owns the `DBT_MANAS_*` schemas; these own everything
dbt and the connector assume already exists.

| File | Purpose | Idempotent |
|---|---|---|
| `01_platform.sql` | Database, schemas, warehouse, roles, external volume, future grants, ingest service user | yes |
| `02_verify.sql` | Health checks: Iceberg, offset contiguity, dynamic-table state | read-only |
| `03_raw_data.sql` | Batch dimension tables + benchmark loader | **no** |
| `04_dbt_project.sql` | dbt version pin, hourly `RUN_DBT` task | yes |
| `landing/<topic>_stream.sql` | One landing table per Kafka topic | yes |

All ACCOUNTADMIN except `02`, which runs as TRANSFORMER.

## Order

```bash
snow sql -c tpch -f snowflake/01_platform.sql            # once per account
snow sql -c tpch -f snowflake/landing/orders_stream.sql  # once per topic
snow sql -c tpch -f snowflake/landing/payments_stream.sql
snow sql -c tpch -f snowflake/04_dbt_project.sql
snow sql -c tpch -f snowflake/02_verify.sql
```

`01_platform.sql` must run before any `landing/` file: a future grant applies
only to objects created after it, so a landing table created first comes back
with no grants and the connector fails on a table that looks healthy.

**Do not run `03_raw_data.sql` on this account** — the dimension tables hold
150,000 / 25 / 5 rows and its INSERTs are not idempotent. To resume suspended
dynamic tables, run only its four `ALTER DYNAMIC TABLE ... RESUME` statements.

## Adding a topic

One connector serves every topic and `snowflake.schema.name` is a single
value, so all topics land in `RAW_STREAMS`, each routed to its own table by
`topic2table.map`. Never deploy a second connector.

1. Copy `landing/_template.sql` to `landing/<topic>_stream.sql`, declare the
   payload shape, run it. No grants needed.
2. Add the topic to `KAFKA_TOPICS` and `KAFKA_TOPIC_TABLE_MAP`, redeploy the
   connector.
3. Config rows + a three-line silver model — see [`../docs/streams.md`](../docs/streams.md).

## Pending

`DBT_PIPE.RAW.ORDERS_STREAM` still exists with the original 675 rows. Nothing
reads it since the move to `RAW_STREAMS`. Drop it once rows are confirmed
landing in the new table:

```sql
DROP TABLE dbt_pipe.raw.orders_stream;
```

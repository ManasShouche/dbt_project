-- Batch dimension tables + the benchmark loader. ACCOUNTADMIN.
--
-- WARNING: the INSERTs are NOT idempotent. Running this against populated
-- tables doubles every row. Section 2's RESUME statements are safe alone.

USE ROLE ACCOUNTADMIN;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw.customer_raw (
    C_CUSTKEY    DECIMAL(38,0),
    C_NAME       STRING,
    C_ADDRESS    STRING,
    C_NATIONKEY  DECIMAL(38,0),
    C_PHONE      STRING,
    C_ACCTBAL    DECIMAL(12,2),
    C_MKTSEGMENT STRING,
    C_COMMENT    STRING,
    _LOADED_AT   TIMESTAMP_NTZ(6)
)
    EXTERNAL_VOLUME = 'EV_ICEBERG' CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'raw/customer_raw_v2/' ICEBERG_VERSION = 2
    CHANGE_TRACKING = TRUE;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw.nation_raw (
    N_NATIONKEY DECIMAL(38,0),
    N_NAME      STRING,
    N_REGIONKEY DECIMAL(38,0),
    N_COMMENT   STRING,
    _LOADED_AT  TIMESTAMP_NTZ(6)
)
    EXTERNAL_VOLUME = 'EV_ICEBERG' CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'raw/nation_raw_v2/' ICEBERG_VERSION = 2
    CHANGE_TRACKING = TRUE;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw.region_raw (
    R_REGIONKEY DECIMAL(38,0),
    R_NAME      STRING,
    R_COMMENT   STRING,
    _LOADED_AT  TIMESTAMP_NTZ(6)
)
    EXTERNAL_VOLUME = 'EV_ICEBERG' CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'raw/region_raw_v2/' ICEBERG_VERSION = 2
    CHANGE_TRACKING = TRUE;

INSERT INTO dbt_pipe.raw.customer_raw
SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal,
       c_mktsegment, c_comment, current_timestamp()
FROM snowflake_sample_data.tpch_sf1.customer;

INSERT INTO dbt_pipe.raw.nation_raw
SELECT n_nationkey, n_name, n_regionkey, n_comment, current_timestamp()
FROM snowflake_sample_data.tpch_sf1.nation;

INSERT INTO dbt_pipe.raw.region_raw
SELECT r_regionkey, r_name, r_comment, current_timestamp()
FROM snowflake_sample_data.tpch_sf1.region;

GRANT OWNERSHIP ON ICEBERG TABLE dbt_pipe.raw.customer_raw TO ROLE transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ICEBERG TABLE dbt_pipe.raw.nation_raw   TO ROLE transformer COPY CURRENT GRANTS;
GRANT OWNERSHIP ON ICEBERG TABLE dbt_pipe.raw.region_raw   TO ROLE transformer COPY CURRENT GRANTS;

-- Section 2: resume dynamic tables. Required after any source is recreated;
-- Snowflake does not resume them automatically and dbt builds stay green.
ALTER DYNAMIC TABLE dbt_pipe.dbt_manas_silver.silver_nations   RESUME;
ALTER DYNAMIC TABLE dbt_pipe.dbt_manas_silver.silver_regions   RESUME;
ALTER DYNAMIC TABLE dbt_pipe.dbt_manas_silver.silver_customers RESUME;
ALTER DYNAMIC TABLE dbt_pipe.dbt_manas_gold.dim_customers      RESUME;

SELECT table_name, is_iceberg, row_count
FROM dbt_pipe.information_schema.tables
WHERE table_schema = 'RAW'
ORDER BY table_name;

SHOW DYNAMIC TABLES IN DATABASE dbt_pipe;
SELECT "name", "schema_name", "scheduling_state", "target_lag"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- Section 3: optional benchmark loader. No dbt model reads its output.
CREATE OR REPLACE PROCEDURE "LOAD_MONTH"("MONTH_START" DATE, "NATIVE_TOO" BOOLEAN DEFAULT TRUE)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
declare
    orders_loaded   integer;
    lineitem_loaded integer;
begin
    insert into orders_raw
    select o.o_orderkey, o.o_custkey, o.o_orderstatus, o.o_totalprice,
           o.o_orderdate, o.o_orderpriority, o.o_clerk, o.o_shippriority,
           o.o_comment, current_timestamp()
    from snowflake_sample_data.tpch_sf1.orders o
    where o.o_orderdate >= :month_start
      and o.o_orderdate <  dateadd(month, 1, :month_start);

    orders_loaded := sqlrowcount;

    insert into lineitem_raw
    select l.l_orderkey, l.l_partkey, l.l_suppkey, l.l_linenumber,
           l.l_quantity, l.l_extendedprice, l.l_discount, l.l_tax,
           l.l_returnflag, l.l_linestatus, l.l_shipdate, l.l_commitdate,
           l.l_receiptdate, l.l_shipinstruct, l.l_shipmode, l.l_comment,
           current_timestamp()
    from snowflake_sample_data.tpch_sf1.lineitem l
    join snowflake_sample_data.tpch_sf1.orders o
      on l.l_orderkey = o.o_orderkey
    where o.o_orderdate >= :month_start
      and o.o_orderdate <  dateadd(month, 1, :month_start);

    lineitem_loaded := sqlrowcount;

    if (native_too) then
        insert into dbt_pipe.control.orders_raw_native
        select o.o_orderkey, o.o_custkey, o.o_orderstatus, o.o_totalprice,
               o.o_orderdate, o.o_orderpriority, o.o_clerk, o.o_shippriority,
               o.o_comment, current_timestamp()
        from snowflake_sample_data.tpch_sf1.orders o
        where o.o_orderdate >= :month_start
          and o.o_orderdate <  dateadd(month, 1, :month_start);
    end if;

    return ''loaded '' || :month_start::string
           || '' -- orders: '' || orders_loaded::string
           || '', lineitems: '' || lineitem_loaded::string;
end;
';

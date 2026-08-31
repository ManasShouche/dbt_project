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

TRUNCATE TABLE dbt_pipe.raw.customer_raw;
TRUNCATE TABLE dbt_pipe.raw.nation_raw;
TRUNCATE TABLE dbt_pipe.raw.region_raw;

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

SELECT table_name, is_iceberg, row_count
FROM dbt_pipe.information_schema.tables
WHERE table_schema = 'RAW'
ORDER BY table_name;

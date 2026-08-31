-- Copy to <stream>_stream.sql, replace <STREAM>, declare the payload shape.
-- Run 01_platform.sql first; its future grants cover this table on creation.
--
-- Rules: RECORD_CONTENT must be a structured OBJECT (VARIANT is rejected for
-- Iceberg). Money must carry scale, NUMBER(_,2). RECORD_METADATA must be
-- declared with exactly the eight fields below, camelCase, or rows land with
-- no lineage and no error. Field names are case-sensitive.

USE ROLE ACCOUNTADMIN;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw_streams.<STREAM>_stream (
    RECORD_CONTENT OBJECT(
        -- payload fields go here
        _loaded_at TIMESTAMP_LTZ
    ),
    RECORD_METADATA OBJECT(
        topic                      STRING,
        partition                  NUMBER(38,0),
        offset                     NUMBER(38,0),
        key                        STRING,
        CreateTime                 NUMBER(38,0),
        LogAppendTime              NUMBER(38,0),
        SnowflakeConnectorPushTime NUMBER(38,0),
        headers                    MAP(VARCHAR, VARCHAR)
    )
)
    EXTERNAL_VOLUME = 'EV_ICEBERG'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'raw_streams/<STREAM>_stream/'
    ICEBERG_VERSION = 3
    CHANGE_TRACKING = TRUE
    COMMENT = 'Iceberg raw landing for streamed <STREAM> events';

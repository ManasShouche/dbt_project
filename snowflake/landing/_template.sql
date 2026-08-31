USE ROLE ACCOUNTADMIN;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw_streams.<STREAM>_stream (
    RECORD_CONTENT OBJECT(

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

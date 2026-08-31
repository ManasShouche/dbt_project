USE ROLE ACCOUNTADMIN;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw_streams.payments_stream (
    RECORD_CONTENT OBJECT(
        payment_id      NUMBER(38,0),
        order_id        NUMBER(38,0),
        amount          NUMBER(12,2),
        payment_method  STRING,
        payment_status  STRING,
        payment_ts      TIMESTAMP_LTZ,
        _loaded_at      TIMESTAMP_LTZ
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
    BASE_LOCATION = 'raw_streams/payments_stream/'
    ICEBERG_VERSION = 3
    CHANGE_TRACKING = TRUE
    COMMENT = 'Iceberg raw landing for streamed payment events';

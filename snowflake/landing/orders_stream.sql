USE ROLE ACCOUNTADMIN;

CREATE ICEBERG TABLE IF NOT EXISTS dbt_pipe.raw_streams.orders_stream (
    RECORD_CONTENT OBJECT(
        order_key       NUMBER(38,0),
        order_status    STRING,
        total_price     NUMBER(12,2),
        order_date      DATE,
        order_priority  STRING,
        clerk_id        STRING,
        ship_priority   NUMBER(38,0),
        order_comment   STRING,
        customer OBJECT(
            customer_key    NUMBER(38,0),
            name            STRING,
            nation_key      NUMBER(38,0),
            market_segment  STRING
        ),
        line_items ARRAY(OBJECT(
            line_number     NUMBER(38,0),
            part_key        NUMBER(38,0),
            supplier_key    NUMBER(38,0),
            quantity        NUMBER(38,0),
            extended_price  NUMBER(12,2),
            discount        NUMBER(12,2),
            tax             NUMBER(12,2),
            return_flag     STRING,
            line_status     STRING,
            ship_date       DATE,
            commit_date     DATE,
            receipt_date    DATE,
            ship_instruct   STRING,
            ship_mode       STRING
        )),
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
    BASE_LOCATION = 'raw_streams/orders_stream/'
    ICEBERG_VERSION = 3
    CHANGE_TRACKING = TRUE
    COMMENT = 'Iceberg raw landing for streamed order events';

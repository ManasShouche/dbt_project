{% macro build_silver_stream(stream_name) %}

{%- if not execute or stream_config_optional() -%}
select 1 as parse_time_placeholder
{%- else -%}

{%- set cfg     = get_stream_config(stream_name) -%}
{%- set columns = get_stream_columns(stream_name) -%}

{%- set payload   = cfg.get('payload_column', 'record_content') -%}
{%- set watermark = cfg.get('watermark_field', 'ingested_at') -%}
{%- set pk        = cfg.get('primary_key') -%}

with bronze as (

    select * from {{ ref(cfg.get('bronze_model')) }}

    {% if is_incremental() %}

    where {{ watermark }} > (
        select dateadd(hour, -{{ var('late_arrival_hours') }}, max({{ watermark }}))
        from {{ this }}
    )
    {% endif %}

),

expanded as (

    select
        {%- for col in columns %}
        {{ payload }}:{{ col['source_field'] }}::{{ col['data_type'] }} as {{ col['target_field'] }}{{ "," if not loop.last }}
        {%- endfor %},

        {{ watermark }},
        kafka_offset

    from bronze

),

deduplicated as (

    select
        {%- for col in columns %}
        {{ col['target_field'] }},
        {%- endfor %}
        {{ watermark }}

    from expanded

    qualify row_number() over (
        partition by {{ pk }}
        order by {{ watermark }}  desc nulls last,
                 kafka_offset     desc nulls last
    ) = 1

)

select * from deduplicated

{%- endif -%}
{% endmacro %}

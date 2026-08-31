{{ config(materialized = 'view') }}

-- depends_on: {{ ref('stream_config') }}
-- depends_on: {{ ref('stream_column_config') }}
-- depends_on: {{ ref('silver_orders') }}
-- depends_on: {{ ref('silver_payments') }}

{%- set streams = get_enabled_streams() %}

{%- if streams | length == 0 %}

select
    null::varchar    as stream_name,
    null::varchar    as source_topic,
    null::varchar    as raw_table,
    null::number     as row_count,
    null::timestamp_ntz as last_event_at,
    null::number     as minutes_behind,
    null::number     as sla_minutes,
    null::boolean    as is_within_sla
where false

{%- else %}

{% for s in streams %}
select
    '{{ s["stream_name"] }}'                        as stream_name,
    '{{ s["source_topic"] }}'                       as source_topic,
    '{{ s["raw_table"] }}'                          as raw_table,
    count(*)                                        as row_count,
    max({{ s["watermark_field"] }})                 as last_event_at,
    datediff('minute', max({{ s["watermark_field"] }}), current_timestamp())
                                                    as minutes_behind,
    {{ s["sla_minutes"] }}                          as sla_minutes,
    datediff('minute', max({{ s["watermark_field"] }}), current_timestamp())
        <= {{ s["sla_minutes"] }}                   as is_within_sla

from {{ ref(s["target_model"]) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}

{%- endif %}

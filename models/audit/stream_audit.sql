{{ config(materialized = 'view') }}

-- One row per enabled stream: how much is in it, how fresh it is, and
-- whether that is inside the SLA the config declares.
--
-- A view, so it is answered at query time rather than being another thing
-- that can go stale. minutes_behind is measured against the watermark, so it
-- reads "how long since anything new reached us".
--
-- Run failures are deliberately out of scope -- dbt's run_results.json and
-- Snowflake's TASK_HISTORY already own those.

-- One depends_on per enabled stream. Required: the target models cannot be
-- discovered from config before a connection exists.
-- depends_on: {{ ref('stream_config') }}
-- depends_on: {{ ref('silver_orders') }}
-- depends_on: {{ ref('silver_payments') }}

{%- set streams = get_enabled_streams() %}

{%- if streams | length == 0 %}

{#- Nothing enabled. Emit a shaped-but-empty result rather than invalid SQL,
    so the model still builds and the tests on it still mean something. -#}
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

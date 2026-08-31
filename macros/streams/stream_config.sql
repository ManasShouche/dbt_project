{% macro _rows_as_dicts(results) %}
    {%- set out = [] -%}
    {%- set names = results.column_names | map('lower') | list -%}
    {%- for row in results.rows -%}
        {%- set d = {} -%}
        {%- for name in names -%}
            {%- do d.update({name: row[loop.index0]}) -%}
        {%- endfor -%}
        {%- do out.append(d) -%}
    {%- endfor -%}
    {{ return(out) }}
{% endmacro %}

{% macro get_stream_config(stream_name) %}
    {%- if not execute -%}{{ return({}) }}{%- endif -%}

    {%- set sql -%}
        select *
        from {{ ref('stream_config') }}
        where lower(stream_name) = lower('{{ stream_name }}')
          and enabled
    {%- endset -%}

    {%- set rows = _rows_as_dicts(run_query(sql)) -%}

    {%- if rows | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "No enabled row in stream_config for stream '" ~ stream_name ~ "'. "
            ~ "Add it to seeds/config/stream_config.csv (enabled=true) and run `dbt seed`."
        ) }}
    {%- endif -%}

    {{ return(rows[0]) }}
{% endmacro %}

{% macro get_stream_columns(stream_name) %}
    {%- if not execute -%}{{ return([]) }}{%- endif -%}

    {%- set sql -%}
        select source_field, target_field, data_type
        from {{ ref('stream_column_config') }}
        where lower(stream_name) = lower('{{ stream_name }}')
        order by ordinal
    {%- endset -%}

    {%- set rows = _rows_as_dicts(run_query(sql)) -%}

    {%- if rows | length == 0 -%}
        {{ exceptions.raise_compiler_error(
            "No rows in stream_column_config for stream '" ~ stream_name ~ "'. "
            ~ "A stream with no field mappings would build a table with no columns."
        ) }}
    {%- endif -%}

    {{ return(rows) }}
{% endmacro %}

{% macro get_enabled_streams() %}
    {%- if not execute -%}{{ return([]) }}{%- endif -%}

    {%- set sql -%}
        select stream_name, source_topic, raw_table, target_model,
               primary_key, watermark_field, sla_minutes
        from {{ ref('stream_config') }}
        where enabled
        order by stream_name
    {%- endset -%}

    {{ return(_rows_as_dicts(run_query(sql))) }}
{% endmacro %}

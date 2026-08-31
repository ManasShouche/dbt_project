-- One mapping per (stream, target_field). A duplicate emits the same column
-- twice in the generated SELECT, and the model fails with a duplicate-column
-- error that names the column but not the config row that caused it.

select
    stream_name,
    target_field,
    count(*) as copies

from {{ ref('stream_column_config') }}

group by stream_name, target_field
having count(*) > 1

select
    stream_name,
    target_field,
    count(*) as copies

from {{ ref('stream_column_config') }}

group by stream_name, target_field
having count(*) > 1

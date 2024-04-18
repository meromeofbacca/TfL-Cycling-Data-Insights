{{ config(materialized='view') }}
 
with cycling_data as 
(
  select *,
    row_number() over(partition by unqid, datetime) as rn
  from {{ source('staging','cycleways_cycling_partitioned') }}
  where unqid is not null 
)
select
   -- identifiers
    {{ dbt_utils.generate_surrogate_key(['unqid', 'datetime']) }} as count_id,
    counting_period,
    unqid,
    weather,
    day,
    round,
    dir,
    path,
    mode,
    {{ dbt.safe_cast("count", api.Column.translate_type("integer")) }} as count,
    cast(datetime as timestamp) as datetime
from cycling_data
where rn = 1

-- dbt build --select stg_inner_cycling_partitioned --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
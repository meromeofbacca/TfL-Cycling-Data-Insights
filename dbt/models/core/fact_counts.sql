{{
    config(
        materialized='table'
    )
}}

with central_data as (
    select *, 
        'Central' as area
    from {{ ref('stg_central_cycling_partitioned') }}
), 
cycleways_data as (
    select *, 
        'Cycleways' as area
    from {{ ref('stg_cycleways_cycling_partitioned') }}
), 
inner_data as (
    select *, 
        'Inner' as area
    from {{ ref('stg_inner_cycling_partitioned') }}
), 
outer_data as (
    select *, 
        'Outer' as area
    from {{ ref('stg_outer_cycling_partitioned') }}
), 
counts_unioned as (
    select * from central_data
    union all 
    select * from cycleways_data
    union all
    select * from inner_data
    union all
    select * from outer_data
), 
monitoring_locations as (
    select * from {{ ref('monitoring_locations') }}
)
select 
    count_id,    
    counting_period,
    unqid,
    weather,
    day,
    round,
    dir,
    path,
    mode,
    count,
    datetime,
    area
from counts_unioned
inner join monitoring_locations 
on counts_unioned.unqid = monitoring_locations.site_id
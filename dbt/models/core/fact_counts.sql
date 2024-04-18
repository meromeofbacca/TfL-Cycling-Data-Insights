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
    counts_unioned.count_id,    
    counts_unioned.counting_period,
    counts_unioned.unqid,
    counts_unioned.weather,
    counts_unioned.day,
    counts_unioned.round,
    counts_unioned.dir,
    counts_unioned.path,
    counts_unioned.mode,
    counts_unioned.count,
    counts_unioned.datetime,
    counts_unioned.area,
    monitoring_locations.location_description,
    monitoring_locations.borough,
    monitoring_locations.functional_area_for_monitoring,
    monitoring_locations.road_type,
    monitoring_locations.is_it_on_the_strategic_cio_panel,
    monitoring_locations.old_site_id_legacy,
    monitoring_locations.easting_uk_grid,
    monitoring_locations.northing_uk_grid,
    monitoring_locations.latitude,
    monitoring_locations.longitude

from counts_unioned
inner join monitoring_locations 
on counts_unioned.unqid = monitoring_locations.site_id
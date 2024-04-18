{{ config(materialized='table') }}


with source as (

    select * from {{ source('staging', 'monitoring_locations') }}
    where site_id is not null
),

renamed as (

    select
        -- identifiers
        site_id,

        location_description,
        borough,
        functional_area_for_monitoring,
        road_type,
        {{ dbt.safe_cast("is_it_on_the_strategic_cio_panel", api.Column.translate_type("integer")) }} as is_it_on_the_strategic_cio_panel,
        old_site_id_legacy,
        easting_uk_grid,
        northing_uk_grid,
        latitude,
        longitude

    from source

)

select * from renamed

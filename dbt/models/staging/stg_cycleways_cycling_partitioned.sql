with 

source as (

    select * from {{ source('staging', 'cycleways_cycling_partitioned') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['unqid', 'datetime']) }} as count_id,
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

    from source

)

select * from renamed

CREATE OR REPLACE TABLE `de-zoomcamp-420207.uk_cycling.{{area}}_cycling_partitioned`
    PARTITION BY DATE(datetime)
    CLUSTER BY unqid AS (
    SELECT * FROM {{df_1}}
    );
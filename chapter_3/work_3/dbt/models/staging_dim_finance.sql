{{ config(
    materialized='table',
    schema='staging',
    alias='dim_finance',
    tags=['staging']
) }}

WITH source_data AS (
    SELECT
        unique_id,
        clabe
    FROM
        {{ source('raw_source', 'raw_batch_data') }}
)

SELECT
    *
FROM
    source_data
    
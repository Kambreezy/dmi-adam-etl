{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['country']},
    ]
)}}

SELECT
    DISTINCT country,
    current_date AS load_date
FROM
    {{ ref('dim_sub_county') }} as countries
ORDER BY
    country

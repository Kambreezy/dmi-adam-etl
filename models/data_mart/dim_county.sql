{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['country']},
      {'columns': ['county']},
    ]
)}}

SELECT
    country,
    county,
    current_date AS load_date
FROM
    {{ ref('dim_sub_county') }} as counties
GROUP BY
    country, county
ORDER BY
    county

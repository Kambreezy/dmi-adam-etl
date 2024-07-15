{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['country']},
        {'columns': ['county']},
        {'columns': ['subcounty']},
    ]
)}}

SELECT
    country,
    substring(subcounties.county FROM '^(.*?) County') AS county,
    substring(subcounties.unit_name FROM '^(.*?) Sub County') AS subcounty,
    current_date AS load_date
FROM {{ ref('kenya_sub_counties') }} AS subcounties
ORDER BY
    subcounty

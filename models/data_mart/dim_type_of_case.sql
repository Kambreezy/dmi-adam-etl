{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['type_of_case']},
    ]
)}}

SELECT
    DISTINCT type_of_case AS type_of_case,
    current_date AS load_date
FROM
    {{ ref('fct_case_linelist') }} as linelist
WHERE type_of_case IS NOT NULL
ORDER BY
    type_of_case

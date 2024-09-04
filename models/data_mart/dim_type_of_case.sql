{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['name']},
    ]
)}}

SELECT
    DISTINCT type_of_case AS name,
    current_date AS load_date
FROM
    {{ ref('fct_case_linelist') }} as linelist
ORDER BY
    type_of_case

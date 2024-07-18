{{ config(
    materialized = 'table',
    indexes=[
        {'columns': ['syndrome']},
        {'columns': ['disease']},
        {'columns': ['case_date']},
        {'columns': ['epi_week']},
        {'columns': ['country']},
        {'columns': ['county']},
        {'columns': ['subcounty']},
    ]
)}}

WITH case_linelist_by_date_diarrhoeal_diseases AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        CASE
            WHEN linelist.syndrome IS NULL THEN 'Diarrhoeal Disease'
            ELSE linelist.syndrome
        END AS syndrome,
        disease,
        country,
        county,
        subcounty,
        CASE
            WHEN linelist.case_date IS NOT NULL THEN 1
            ELSE 0
        END AS cases,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.syndrome = 'Diarrhoeal Disease'
), case_linelist_by_date_measles AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        null::text AS syndrome,
        CASE
            WHEN linelist.disease IS NULL THEN 'Measles'
            ELSE linelist.disease
        END AS disease,
        country,
        county,
        subcounty,
        CASE
            WHEN linelist.case_date IS NOT NULL THEN 1
            ELSE 0
        END AS cases,
        current_date AS load_date
    FROM {{ ref('dim_date') }} AS dim_date
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.disease = 'Measles'
)

SELECT * FROM case_linelist_by_date_diarrhoeal_diseases
UNION
SELECT * FROM case_linelist_by_date_measles
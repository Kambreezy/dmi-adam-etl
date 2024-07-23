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

WITH case_linelist_by_date_acute_flaccid_paralysis AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        CASE
            WHEN linelist.disease IS NULL THEN 'AFP'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.disease = 'AFP'
), case_linelist_by_date_community_led_total_sanitation AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        CASE
            WHEN linelist.syndrome IS NULL THEN 'Community Led Total Sanitation'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.syndrome = 'Community Led Total Sanitation'
), case_linelist_by_date_diarrhoeal_diseases AS (
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
        syndrome,
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
), case_linelist_by_date_meningitis AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        CASE
            WHEN linelist.disease IS NULL THEN 'Meningitis'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.disease = 'Meningitis'
), case_linelist_by_date_neonatal_tetanus AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        CASE
            WHEN linelist.disease IS NULL THEN 'Neonatal Tetanus'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.disease = 'Neonatal Tetanus'
), case_linelist_by_date_rabies AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        syndrome,
        CASE
            WHEN linelist.disease IS NULL THEN 'Rabies'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.disease = 'Rabies'
), case_linelist_by_date_respiratory_syndrome AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        CASE
            WHEN linelist.syndrome IS NULL THEN 'Respiratory Syndrome'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.syndrome = 'Respiratory Syndrome'
), case_linelist_by_date_sampling_form_for_fortified_foods AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        CASE
            WHEN linelist.syndrome IS NULL THEN 'Sampling Form for Fortified Foods'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.syndrome = 'Sampling Form for Fortified Foods'
), case_linelist_by_date_viral_hemorrhagic_fever AS (
    SELECT
        dim_date.case_date,
        dim_date.epi_week,
        CASE
            WHEN linelist.syndrome IS NULL THEN 'VHF'
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
    LEFT JOIN {{ ref('fct_case_linelist') }} AS linelist ON dim_date.case_date = linelist.case_date AND linelist.syndrome = 'VHF'
)

SELECT * FROM case_linelist_by_date_acute_flaccid_paralysis
UNION
SELECT * FROM case_linelist_by_date_community_led_total_sanitation
UNION
SELECT * FROM case_linelist_by_date_diarrhoeal_diseases
UNION
SELECT * FROM case_linelist_by_date_measles
UNION
SELECT * FROM case_linelist_by_date_meningitis
UNION
SELECT * FROM case_linelist_by_date_neonatal_tetanus
UNION
SELECT * FROM case_linelist_by_date_rabies
UNION
SELECT * FROM case_linelist_by_date_respiratory_syndrome
UNION
SELECT * FROM case_linelist_by_date_sampling_form_for_fortified_foods
UNION
SELECT * FROM case_linelist_by_date_viral_hemorrhagic_fever

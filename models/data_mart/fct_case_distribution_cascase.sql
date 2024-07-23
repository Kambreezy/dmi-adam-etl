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
      {'columns': ['category']},
    ]
)}}

WITH suspected_acute_flaccid_paralysis AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Suspected' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'AFP'
    GROUP BY case_date
),
tested_acute_flaccid_paralysis AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Tested' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'AFP' AND tested = 1
    GROUP BY case_date
),
confirmed_acute_flaccid_paralysis AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Confirmed' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'AFP' AND confirmed = 1
    GROUP BY case_date
),
admitted_acute_flaccid_paralysis AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Admitted' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'AFP' AND admitted = 1
    GROUP BY case_date
),
recovered_acute_flaccid_paralysis AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Recovered' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'AFP' AND discharged = 1
    GROUP BY case_date
),
died_acute_flaccid_paralysis AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Died' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'AFP' AND died = 1
    GROUP BY case_date
),
suspected_diarrhoeal_disease_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Suspected' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE syndrome = 'Diarrhoeal Disease'
    GROUP BY case_date
),
tested_diarrhoeal_disease_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Tested' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE syndrome = 'Diarrhoeal Disease' AND tested = 1
    GROUP BY case_date
),
confirmed_diarrhoeal_disease_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Confirmed' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE syndrome = 'Diarrhoeal Disease' AND confirmed = 1
    GROUP BY case_date
),
admitted_diarrhoeal_disease_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Admitted' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE syndrome = 'Diarrhoeal Disease' AND admitted = 1
    GROUP BY case_date
),
recovered_diarrhoeal_disease_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Recovered' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE syndrome = 'Diarrhoeal Disease' AND discharged = 1
    GROUP BY case_date
),
died_diarrhoeal_disease_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Died' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE syndrome = 'Diarrhoeal Disease' AND died = 1
    GROUP BY case_date
),
suspected_measles_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Suspected' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'Measles'
    GROUP BY case_date
),
tested_measles_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Tested' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'Measles' AND tested = 1
    GROUP BY case_date
),
confirmed_measles_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Confirmed' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'Measles' AND confirmed = 1
    GROUP BY case_date
),
admitted_measles_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Admitted' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'Measles' AND admitted = 1
    GROUP BY case_date
),
recovered_measles_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Recovered' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'Measles' AND discharged = 1
    GROUP BY case_date
),
died_measles_cases AS (
    SELECT 
        MAX(syndrome) AS syndrome,
        MAX(disease) AS disease,
        case_date,
        MAX(epi_week) AS epi_week,
        MAX(country) AS country,
        MAX(county) AS county,
        MAX(subcounty) AS subcounty,
        'Died' as category,
        COUNT(*) as cases,
        current_date AS load_date
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease = 'Measles' AND died = 1
    GROUP BY case_date
)

SELECT * FROM suspected_diarrhoeal_disease_cases
UNION
SELECT * FROM tested_diarrhoeal_disease_cases
UNION
SELECT * FROM confirmed_diarrhoeal_disease_cases
UNION
SELECT * FROM admitted_diarrhoeal_disease_cases
UNION
SELECT * FROM recovered_diarrhoeal_disease_cases
UNION
SELECT * FROM died_diarrhoeal_disease_cases
UNION
SELECT * FROM suspected_measles_cases
UNION
SELECT * FROM tested_measles_cases
UNION
SELECT * FROM confirmed_measles_cases
UNION
SELECT * FROM admitted_measles_cases
UNION
SELECT * FROM recovered_measles_cases
UNION
SELECT * FROM died_measles_cases
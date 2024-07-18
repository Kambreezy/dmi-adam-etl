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

WITH suspected_diarrhoeal_disease_cases AS (
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
    WHERE
        syndrome = 'Diarrhoeal Disease'
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
    WHERE
        syndrome = 'Diarrhoeal Disease'
    AND
        rdt_done = 'Yes'
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
    WHERE
        syndrome = 'Diarrhoeal Disease'
    AND
        rdt_results = 'Positive'
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
    WHERE
        syndrome = 'Diarrhoeal Disease'
    AND
        status_of_patient = 'Admitted'
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
    WHERE
        syndrome = 'Diarrhoeal Disease'
    AND
        status_of_patient = 'Recovered'
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
    WHERE
        syndrome = 'Diarrhoeal Disease'
    AND
        outcome_of_patient = 'Dead'
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

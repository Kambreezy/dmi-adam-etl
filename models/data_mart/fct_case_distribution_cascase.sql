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

{% set categories = ['Suspected', 'Tested', 'Confirmed', 'Admitted', 'Recovered', 'Died'] %}
{% set diseases = ['\'AFP\'', '\'Measles\'', '\'Meningitis\'', '\'Neonatal Tetanus\'', '\'Rabies\''] %}
{% set syndromes = ['\'Diarrhoeal Disease\'', '\'Respiratory Syndrome\'', '\'VHF\''] %}

WITH base_data AS (
    SELECT 
        syndrome,
        disease,
        case_date,
        epi_week,
        country,
        county,
        subcounty,
        COUNT(*) AS suspected,
        SUM(CASE WHEN tested = 1 THEN 1 ELSE 0 END) AS tested,
        SUM(CASE WHEN confirmed = 1 THEN 1 ELSE 0 END) AS confirmed,
        SUM(CASE WHEN admitted = 1 THEN 1 ELSE 0 END) AS admitted,
        SUM(CASE WHEN discharged = 1 THEN 1 ELSE 0 END) AS recovered,
        SUM(CASE WHEN died = 1 THEN 1 ELSE 0 END) AS died
    FROM {{ ref('fct_case_linelist') }}
    WHERE disease IN ({{ diseases | join(', ') }}) OR syndrome IN ({{ syndromes | join(', ') }})
    GROUP BY syndrome, disease, case_date, epi_week, country, county, subcounty
),

final_data AS (
    {% for category in categories %}
    SELECT 
        syndrome,
        disease,
        case_date,
        epi_week,
        country,
        county,
        subcounty,
        '{{ category }}' AS category,
        {% if category == 'Suspected' %}
        COALESCE(suspected, 0) AS cases
        {% elif category == 'Tested' %}
        COALESCE(tested, 0) AS cases
        {% elif category == 'Confirmed' %}
        COALESCE(confirmed, 0) AS cases
        {% elif category == 'Admitted' %}
        COALESCE(admitted, 0) AS cases
        {% elif category == 'Recovered' %}
        COALESCE(recovered, 0) AS cases
        {% elif category == 'Died' %}
        COALESCE(died, 0) AS cases
        {% endif %}
    FROM base_data

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

SELECT 
    syndrome,
    disease,
    case_date,
    epi_week,
    country,
    county,
    subcounty,
    category,
    cases,
    current_date AS load_date
FROM final_data
ORDER BY syndrome, disease, case_date DESC, category
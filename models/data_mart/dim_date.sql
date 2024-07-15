{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['case_date']},
      {'columns': ['year']},
      {'columns': ['month']},
      {'columns': ['day']},
      {'columns': ['quarter']},
      {'columns': ['week']},
      {'columns': ['epi_week']},
    ]
)}}

SELECT
    date AS case_date,
    date_part('year', date) AS year,
    date_part('month', date) AS month,
    date_part('day', date) AS day,
    date_part('quarter', date) AS quarter,
    date_part('week', date) AS week,
    to_char(date, 'YYYY "W"IW') AS epi_week,
    current_date AS load_date
FROM
    {{ ref('stg_date_range') }}
ORDER BY
    date DESC

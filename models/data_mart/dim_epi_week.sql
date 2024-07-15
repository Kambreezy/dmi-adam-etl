{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['epi_week']},
      {'columns': ['year']},
      {'columns': ['week']},
      {'columns': ['week_start_date']},
      {'columns': ['week_end_date']},
    ]
)}}

WITH epi_weeks AS (
    SELECT
        distinct date_trunc('week', date) - interval '1 day' AS start_of_week,
        date_trunc('week', date) + interval '5 days' AS end_of_week
    FROM
        {{ ref('stg_date_range') }}
),
final AS (
    SELECT
        start_of_week::date AS week_start_date,
        end_of_week::date AS week_end_date,
        CASE
            WHEN date_part('month', end_of_week) = 1
            AND date_part('week', end_of_week) in (52, 53) then date_part('year', end_of_week) - 1
            ELSE date_part('year', end_of_week)
        END AS year,
        date_part('week', end_of_week) AS week
    FROM
        epi_weeks
)
SELECT
    concat(year, ' W', LPAD(week::text, 2, '0')) AS epi_week,
    year,
    week,
    week_start_date,
    week_end_date,
    current_date AS load_date
FROM
    final
ORDER BY
    week_start_date DESC
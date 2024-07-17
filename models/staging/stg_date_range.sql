SELECT
    generate_series(
        '{{ var("start_date") }}' :: date,
        '{{ run_started_at.strftime("%Y-%m-%d") }}'::date,
        '1 day' :: interval
    )::date AS date

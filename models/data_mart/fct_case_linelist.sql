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
      {'columns': ['suspected']},
      {'columns': ['tested']},
      {'columns': ['confirmed']},
      {'columns': ['admitted']},
      {'columns': ['discharged']},
      {'columns': ['died']},
    ]
)}}

WITH acute_flaccid_paralysis AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN date_first_specimen_collected IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN afp_final_case_classification = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN outcome = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN outcome = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_acute_flaccid_paralysis') }} AS int_acute_flaccid_paralysis
    WHERE case_date IS NOT NULL
), community_led_total_sanitation AS (
    SELECT
        'Community Led Total Sanitation' AS syndrome,
        null::text AS disease,
        case_date,
        epi_week,
        null::text AS sex,
        null::text AS age_group,
        null::text AS country,
        null::text AS county,
        subcounty::text AS subcounty,
        (1)::integer AS suspected,
        (0)::integer AS tested,
        (0)::integer AS confirmed,
        (0)::integer AS admitted,
        (0)::integer AS discharged,
        (0)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_community_led_total_sanitation') }}
    WHERE case_date IS NOT NULL
), diarrhoeal_disease AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN rdt_done = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN rdt_results = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM
        {{ ref('int_diarrhoeal_disease') }}  as int_diarrhoeal_disease
    WHERE case_date IS NOT NULL
), measles AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN were_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN laboratory_results = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN outcome = 'Admitted' THEN 1 WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN outcome = 'Discharged' THEN 1 WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_measles') }}
    WHERE case_date IS NOT NULL
), meningitis AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN laboratory_test_requested IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN patient_status IN ('Admitted', 'Discharged', 'Dead') THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN patient_status = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_meningitis') }}
    WHERE case_date IS NOT NULL
), monkey_pox AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN outcome_of_patient = 'Admitted' THEN 1 WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN outcome_of_patient = 'Discharged' THEN 1 WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_monkey_pox') }}
    WHERE case_date IS NOT NULL
), neonatal_tetanus AS (
    SELECT
        ''::text AS syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN laboratory_samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN confirmed_nnt = 'Yes' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_neonatal_tetanus') }}
    WHERE case_date IS NOT NULL
), rabies AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN exposure_type_rabies IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN exposure_type_rabies IS NOT NULL THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN admitted = 'Yes' THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_rabies') }}
    WHERE case_date IS NOT NULL
), respiratory_syndrome AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN were_laboratory_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN date_of_admission IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN date_of_discharge IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN date_of_death IS NOT NULL THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_respiratory_syndrome') }}
    WHERE case_date IS NOT NULL
), sampling_form_for_fortified_foods AS (
    SELECT
        'Sampling Form for Fortified Foods' AS syndrome,
        disease,
        case_date,
        epi_week,
        null::text AS sex,
        null::text AS age_group,
        null::text AS country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN date_of_sample_collection IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
        (0)::integer AS confirmed,
        (0)::integer AS admitted,
        (0)::integer AS discharged,
        (0)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_sampling_form_for_fortified_foods') }}
    WHERE case_date IS NOT NULL
), viral_hemorrhagic_fever AS (
    SELECT
        syndrome,
        disease,
        case_date,
        epi_week,
        sex,
        age_group,
        country,
        county,
        subcounty,
        (1)::integer AS suspected,
        (CASE WHEN rdt_done = 'Yes' THEN 1 WHEN laboratory_sample_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
        (CASE WHEN rdt_results = 'Positive' THEN 1 WHEN outcome_final_case_classification = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
        (CASE WHEN admitted = 'Yes' THEN 1 WHEN outcome = 'Admitted' THEN 1 WHEN admission_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
        (CASE WHEN patient_status = 'Discharged' THEN 1 WHEN discharge_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
        (CASE WHEN patient_status = 'Died' THEN 1 WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
        location_accuracy,
        location_latitude,
        location_longitude,
        current_date AS load_date
    FROM {{ ref('int_viral_hemorrhagic_fever') }}
    WHERE case_date IS NOT NULL
)

SELECT * FROM acute_flaccid_paralysis
UNION
SELECT * FROM community_led_total_sanitation
UNION
SELECT * FROM diarrhoeal_disease
UNION
SELECT * FROM measles
UNION
SELECT * FROM meningitis
UNION
SELECT * FROM monkey_pox
UNION
SELECT * FROM neonatal_tetanus
UNION
SELECT * FROM rabies
UNION
SELECT * FROM respiratory_syndrome
UNION
SELECT * FROM sampling_form_for_fortified_foods
UNION
SELECT * FROM viral_hemorrhagic_fever
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

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    null::date AS date_of_birth,
    age_years,
    age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guradian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    record_exposure,
    exposure_type,
    food_name,
    food_consumption_date,
    food_source,
    food_participants_count,
    food_affected_participants_count,
    water_sources,
    water_sources_other,
    toilet_types,
    toilet_types_other,
    travel_origin_country,
    travel_origin_city,
    travel_departure_date,
    travel_destination_country,
    travel_destination_city,
    travel_arrival_date,
    animal_exposure,
    animal_exposure_other,
    animal_species,
    animal_species_other,
    animal_exposure_location,
    null::text AS animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    null::text AS health_facility_name,
    null::text AS date_of_admission,
    null::text AS inpatient_number,
    outcome::text AS outcome_of_patient,
    null::text AS date_of_death,
    null::text AS status_of_patient,
    null::text AS date_of_discharge,
    null::text AS rdt_done,
    null::text AS rdt_results,
    null::text AS laboratory_name,
    null::text AS date_of_sample_collection,
    null::text AS were_samples_collected,
    null::text AS laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    null::text AS vaccinated,
    null::text AS vaccine_doses,
    null::text AS vaccination_date,
    date_of_onset,
    null::text AS symptoms,
    null::text AS symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN date_first_specimen_collected IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN afp_final_case_classification = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN outcome = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN outcome = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_acute_flaccid_paralysis') }} AS int_acute_flaccid_paralysis
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    'Community Led Total Sanitation' AS syndrome,
    null::text AS disease,
    case_date,
    epi_week,
    null::text AS epid,
    null::text AS date_of_investigation,
    null::text AS given_name,
    null::text AS family_name,
    null::text AS sex,
    null::date AS date_of_birth,
    null::float AS age_years,
    null::float AS age_months,
    null::text AS age_group,
    null::text AS country,
    null::text AS county,
    subcounty::text AS subcounty,
    village::text AS town_village_camp,
    null::text AS phone_number,
    null::text AS occupation,
    null::text AS occupation_other,
    null::text AS title_residence,
    null::text AS landmark,
    null::text AS title_parent_guradian,
    null::text AS guardian_family_name,
    null::text AS guardian_given_name,
    null::text AS guardian_phone_number,
    null::text AS title_respondent,
    null::text AS respondent,
    null::text AS respondent_family_name,
    null::text AS respondent_given_name,
    null::text AS respondent_address,
    null::text AS respondent_phone_number,
    null::text AS respondent_relationship,
    null::text AS record_exposure,
    null::text AS exposure_type,
    null::text AS food_name,
    null::text AS food_consumption_date,
    null::text AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    null::text AS travel_origin_country,
    null::text AS travel_origin_city,
    null::text AS travel_departure_date,
    null::text AS travel_destination_country,
    null::text AS travel_destination_city,
    null::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    null::text AS animal_species,
    null::text AS animal_species_other,
    null::text AS animal_exposure_location,
    null::text AS animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    null::text AS health_facility_name,
    null::text AS date_of_admission,
    null::text AS inpatient_number,
    null::text AS outcome_of_patient,
    null::text AS date_of_death,
    null::text AS status_of_patient,
    null::text AS date_of_discharge,    
    null::text AS rdt_done,
    null::text AS rdt_results,
    null::text AS laboratory_name,
    null::text AS date_of_sample_collection,
    null::text AS were_samples_collected,
    null::text AS laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    null::text AS vaccinated,
    null::text AS vaccine_doses,
    null::text AS vaccination_date,
    null::text AS date_of_onset,
    null::text AS symptoms,
    null::text AS symptoms_other,
    (1)::integer AS suspected,
    (0)::integer AS tested,
    (0)::integer AS confirmed,
    (0)::integer AS admitted,
    (0)::integer AS discharged,
    (0)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_community_led_total_sanitation') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    null::date AS date_of_birth,
    age_years,
    age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guradian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    record_exposure,
    exposure_type,
    food_name,
    food_consumption_date,
    food_source,
    food_participants_count,
    food_affected_participants_count,
    water_sources,
    water_sources_other,
    toilet_types,
    toilet_types_other,
    travel_origin_country,
    travel_origin_city,
    travel_departure_date,
    travel_destination_country,
    travel_destination_city,
    travel_arrival_date,
    animal_exposure,
    animal_exposure_other,
    animal_species,
    animal_species_other,
    animal_exposure_location,
    animal_exposure_date,
    type_of_care,
    health_facility_mfl_number,
    health_facility_name,
    date_of_admission,
    inpatient_number,
    outcome_of_patient,
    date_of_death,
    status_of_patient,
    date_of_discharge,    
    rdt_done,
    rdt_results,
    laboratory_name,
    date_of_sample_collection,
    were_samples_collected,
    laboratory_samples_collected,
    laboratory_samples_collected_others,
    vaccinated,
    vaccine_doses,
    vaccination_date,
    date_of_onset,
    symptoms,
    symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN rdt_done = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN rdt_results = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM
    {{ ref('int_diarrhoeal_disease') }}  as int_diarrhoeal_disease
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    null::date as date_of_birth,
    age_years,
    null::float AS age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    null::text AS record_exposure,
    null::text AS exposure_type,
    null::text AS food_name,
    null::text AS food_consumption_date,
    null::text AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    origin_country::text AS travel_origin_country,
    origin_city_town_illage_camp::text AS travel_origin_city,
    date_of_departure::text AS travel_departure_date,
    destination_country::text AS travel_destination_country,
    destination_city_town_village_camp::text AS travel_destination_city,
    date_of_arrival::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    null::text AS animal_species,
    null::text AS animal_species_other,
    null::text AS animal_exposure_location,
    null::text AS animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    null::text AS health_facility_name,
    null::text AS date_of_admission,
    null::text AS inpatient_number,
    outcome::text AS outcome_of_patient,
    date_of_death,
    status_of_patient,
    date_of_discharge,
    were_samples_collected::text AS rdt_done,
    laboratory_results::text AS rdt_results,
    laboratory_facility_name::text AS laboratory_name,
    date_of_sample_collection,
    were_samples_collected,
    laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    vaccination_exists::text AS vaccinated,
    doses_of_vaccine::text AS vaccine_doses,
    date_of_vaccination::text AS vaccination_date,
    date_of_onset,
    symptoms,
    symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN were_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN laboratory_results = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN outcome = 'Admitted' THEN 1 WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN outcome = 'Discharged' THEN 1 WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_measles') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    null::date as date_of_birth,
    age_years,
    null::float AS age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    null::text AS record_exposure,
    null::text AS exposure_type,
    null::text AS food_name,
    null::text AS food_consumption_date,
    null::text AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    null::text AS travel_origin_country,
    null::text AS travel_origin_city,
    null::text AS travel_departure_date,
    null::text AS travel_destination_country,
    null::text AS travel_destination_city,
    null::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    null::text AS animal_species,
    null::text AS animal_species_other,
    null::text AS animal_exposure_location,
    null::text AS animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    null::text AS health_facility_name,
    null::text AS date_of_admission,
    null::text AS inpatient_number,
    null::text AS outcome_of_patient,
    null::text AS date_of_death,
    null::text AS status_of_patient,
    null::text AS date_of_discharge,
    null::text AS rdt_done,
    null::text AS rdt_results,
    null::text AS laboratory_name,
    null::text AS date_of_sample_collection,
    null::text AS were_samples_collected,
    null::text AS laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    null::text AS vaccinated,
    null::text AS vaccine_doses,
    null::text AS vaccination_date,
    date_of_onset,
    symptoms,
    null::text AS symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN laboratory_test_requested IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN patient_status IN ('Admitted', 'Discharged', 'Dead') THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN patient_status = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_meningitis') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    ''::text AS syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    date_of_birth,
    age_years,
    age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    null::text AS record_exposure,
    null::text AS exposure_type,
    null::text AS food_name,
    null::text AS food_consumption_date,
    null::text AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    null::text AS travel_origin_country,
    null::text AS travel_origin_city,
    null::text AS travel_departure_date,
    null::text AS travel_destination_country,
    null::text AS travel_destination_city,
    null::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    null::text AS animal_species,
    null::text AS animal_species_other,
    null::text AS animal_exposure_location,
    null::text AS animal_exposure_date,
    type_of_care,
    mfl_number_of_health_facility::text AS health_facility_mfl_number,
    name_of_health_facility::text AS health_facility_name,
    date_of_admission,
    inpatient_number,
    outcome_of_patient,
    date_of_death,
    status_of_patient,
    date_of_discharge,
    null::text AS rdt_done,
    null::text AS rdt_results,
    name_of_laboratory_facility::text AS laboratory_name,
    date_of_sample_collection,
    laboratory_samples_were_collected::text AS were_samples_collected,
    laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    null::text AS vaccinated,
    null::text AS vaccine_doses,
    null::text AS vaccination_date,
    null::text AS date_of_onset,
    null::text AS symptoms,
    null::text AS symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN laboratory_samples_were_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN confirmed_nnt = 'Yes' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN status_of_patient = 'Admitted' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN status_of_patient = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN outcome_of_patient = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_neonatal_tetanus') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    date_of_birth,
    age_years,
    age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    null::text AS record_exposure,
    exposure_type_rabies AS exposure_type,
    null::text AS food_name,
    null::text AS food_consumption_date,
    null::text AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    null::text AS travel_origin_country,
    null::text AS travel_origin_city,
    null::text AS travel_departure_date,
    null::text AS travel_destination_country,
    null::text AS travel_destination_city,
    null::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    animal_species,
    animal_species_other,
    null::text AS animal_exposure_location,
    exposure_date AS animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    health_facility_name,
    admission_date AS date_of_admission,
    inpatient_number,
    patient_status AS outcome_of_patient,
    null::text AS date_of_death,
    patient_status AS status_of_patient,
    discharge_date AS date_of_discharge,
    null::text AS rdt_done,
    null::text AS rdt_results,
    null::text AS laboratory_name,
    null::text AS date_of_sample_collection,
    null::text AS were_samples_collected,
    null::text AS laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    vaccinated,
    vaccine_doses,
    vaccination_date,
    null::text AS date_of_onset,
    null::text AS symptoms,
    null::text AS symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN exposure_type_rabies IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN exposure_type_rabies IS NOT NULL THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN admitted = 'Yes' THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN patient_status = 'Discharged' THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN patient_status = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_rabies') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    date_of_birth,
    age_years,
    age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    null::text AS record_exposure,
    null::text AS exposure_type,
    null::text AS food_name,
    null::text AS food_consumption_date,
    null::text AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    null::text AS travel_origin_country,
    null::text AS travel_origin_city,
    null::text AS travel_departure_date,
    null::text AS travel_destination_country,
    null::text AS travel_destination_city,
    null::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    null::text AS animal_species,
    null::text AS animal_species_other,
    null::text AS animal_exposure_location,
    null::text AS animal_exposure_date,
    type_of_care,
    health_facility_mfl_number,
    health_facility_name,
    date_of_admission,
    inpatient_number,
    outcome_of_patient,
    date_of_death,
    status_of_patient,
    date_of_discharge,
    were_laboratory_samples_collected AS rdt_done,
    result_of_laboratory_test AS rdt_results,
    laboratory_facility_name AS laboratory_name,
    date_of_sample_collection,
    were_laboratory_samples_collected,
    laboratory_samples_collected,
    laboratory_test_conducted_other AS laboratory_samples_collected_others,
    vaccination_received AS vaccinated,
    number_of_doses AS vaccine_doses,
    date_of_vaccination,
    date_of_onset,
    symptoms,
    symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN were_laboratory_samples_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN result_of_laboratory_test = 'Positive' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN date_of_admission IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN date_of_discharge IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN date_of_death IS NOT NULL THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_respiratory_syndrome') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    'Sampling Form for Fortified Foods' AS syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    null::text AS given_name,
    null::text AS family_name,
    null::text AS sex,
    null::date AS date_of_birth,
    null::float AS age_years,
    null::float AS age_months,
    null::text AS age_group,
    null::text AS country,
    county,
    subcounty,
    ward AS town_village_camp,
    premise_phone_number AS phone_number,
    null::text AS occupation,
    null::text AS occupation_other,
    type_of_premise AS title_residence,
    landmark,
    null::text AS title_parent_guardian,
    null::text AS guardian_family_name,
    null::text AS guardian_given_name,
    null::text AS guardian_phone_number,
    null::text AS title_respondent,
    null::text AS respondent,
    null::text AS respondent_family_name,
    null::text AS respondent_given_name,
    null::text AS respondent_address,
    null::text AS respondent_phone_number,
    null::text AS respondent_relationship,
    null::text AS record_exposure,
    null::text AS exposure_type,
    name_of_product AS food_name,
    null::text AS food_consumption_date,
    name_of_importer_dealer AS food_source,
    null::text AS food_participants_count,
    null::text AS food_affected_participants_count,
    null::text AS water_sources,
    null::text AS water_sources_other,
    null::text AS toilet_types,
    null::text AS toilet_types_other,
    null::text AS travel_origin_country,
    null::text AS travel_origin_city,
    null::text AS travel_departure_date,
    null::text AS travel_destination_country,
    null::text AS travel_destination_city,
    null::text AS travel_arrival_date,
    null::text AS animal_exposure,
    null::text AS animal_exposure_other,
    null::text AS animal_species,
    null::text AS animal_species_other,
    null::text AS animal_exposure_location,
    null::text AS animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    name_of_premise AS health_facility_name,
    null::text AS date_of_admission,
    null::text AS inpatient_number,
    null::text AS outcome_of_patient,
    null::text AS date_of_death,
    null::text AS status_of_patient,
    null::text AS date_of_discharge,
    null::text AS rdt_done,
    null::text AS rdt_results,
    null::text AS laboratory_name,
    date_of_sample_collection,
    null::text AS were_samples_collected,
    type_of_food_sample AS laboratory_samples_collected,
    null::text AS laboratory_samples_collected_others,
    null::text AS vaccinated,
    null::text AS vaccine_doses,
    null::text AS vaccination_date,
    null::text AS date_of_onset,
    null::text AS symptoms,
    null::text AS symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN date_of_sample_collection IS NOT NULL THEN 1 ELSE 0 END)::integer AS tested,
    (0)::integer AS confirmed,
    (0)::integer AS admitted,
    (0)::integer AS discharged,
    (0)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_sampling_form_for_fortified_foods') }}
WHERE case_date IS NOT NULL

UNION

SELECT
    mform_id,
    case_unique_id,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    given_name,
    family_name,
    sex,
    date_of_birth,
    age_years,
    age_months,
    age_group,
    country,
    county,
    subcounty,
    town_village_camp,
    phone_number,
    occupation,
    occupation_other,
    title_residence,
    landmark,
    title_parent_guardian,
    guardian_family_name,
    guardian_given_name,
    guardian_phone_number,
    title_respondent,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_address,
    respondent_phone_number,
    respondent_relationship,
    exposure_record_exposure AS record_exposure,
    exposure_exposure_type AS exposure_type,
    exposure_food_name AS food_name,
    exposure_food_consumption_date AS food_consumption_date,
    exposure_food_source AS food_source,
    exposure_food_participants_count AS food_participants_count,
    exposure_food_affected_participants_count AS food_affected_participants_count,
    exposure_water_sources AS water_sources,
    exposure_water_sources_other AS water_sources_other,
    exposure_toilet_types AS toilet_types,
    exposure_toilet_types_other AS toilet_types_other,
    exposure_travel_origin_country AS travel_origin_country,
    exposure_travel_origin_city AS travel_origin_city,
    exposure_travel_departure_date AS travel_departure_date,
    exposure_travel_destination_country AS travel_destination_country,
    exposure_travel_destination_city AS travel_destination_city,
    exposure_travel_arrival_date AS travel_arrival_date,
    exposure_animal_exposure AS animal_exposure,
    exposure_animal_exposure_other AS animal_exposure_other,
    exposure_animal_species AS animal_species,
    exposure_animal_species_other AS animal_species_other,
    exposure_animal_exposure_location AS animal_exposure_location,
    animal_exposure_date,
    null::text AS type_of_care,
    null::text AS health_facility_mfl_number,
    health_facility_name,
    admission_date AS date_of_admission,
    inpatient_number,
    outcome AS outcome_of_patient,
    null::text AS date_of_death,
    patient_status AS status_of_patient,
    discharge_date AS date_of_discharge,
    rdt_done,
    rdt_results,
    laboratory_name,
    sample_date AS date_of_sample_collection,
    laboratory_sample_collected AS were_samples_collected,
    laboratory_samples_collected,
    laboratory_samples_collected_others,
    null::text AS vaccinated,
    null::text AS vaccine_doses,
    null::text AS vaccination_date,
    symptoms_start_date AS date_of_onset,
    symptoms,
    symptoms_other,
    (1)::integer AS suspected,
    (CASE WHEN rdt_done = 'Yes' THEN 1 WHEN laboratory_sample_collected = 'Yes' THEN 1 ELSE 0 END)::integer AS tested,
    (CASE WHEN rdt_results = 'Positive' THEN 1 WHEN outcome_final_case_classification = 'Confirmed' THEN 1 ELSE 0 END)::integer AS confirmed,
    (CASE WHEN admitted = 'Yes' THEN 1 WHEN outcome = 'Admitted' THEN 1 WHEN admission_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS admitted,
    (CASE WHEN patient_status = 'Discharged' THEN 1 WHEN discharge_date IS NOT NULL THEN 1 ELSE 0 END)::integer AS discharged,
    (CASE WHEN patient_status = 'Died' THEN 1 WHEN outcome = 'Dead' THEN 1 ELSE 0 END)::integer AS died,
    location_accuracy,
    location_latitude,
    location_longitude,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    current_date AS load_date
FROM {{ ref('int_viral_hemorrhagic_fever') }}
WHERE case_date IS NOT NULL

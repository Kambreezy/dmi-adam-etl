{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
      {'columns': ['type_of_case']},
      {'columns': ['country']},
      {'columns': ['county']},
      {'columns': ['subcounty']},
    ]
)}}

SELECT
    mform_id,
    form_id,
    case_unique_id,
    created_username,
    created_timestamp,
    modified_username,
    modified_timestamp,
    location_accuracy,
    location_latitude,
    location_longitude,
    syndrome,
    disease,
    case_date,
    epi_week,
    epid,
    date_of_investigation,
    type_of_case,
    unique_id_of_case,
    given_name,
    family_name,
    sex,
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
    symptoms_other
FROM {{ ref('int_diarrhoeal_disease') }}

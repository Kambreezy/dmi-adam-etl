SELECT
    mform_id::text AS mform_id,
    form_id::text AS form_id,
    case_unique_id::text AS case_unique_id,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    syndrome::text AS syndrome,
    disease::text AS disease,
    CASE 
        WHEN date_of_onset ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_onset, 'DD/MM/YYYY')::date 
        ELSE NULL 
    END AS case_date,
    CASE 
        WHEN date_of_onset ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_onset, 'DD/MM/YYYY HH24:MI:SS'), 'YYYY "W"IW') 
        ELSE NULL
    END AS epi_week,
    epid::text AS epid,
    date_of_investigation::text AS date_of_investigation,
    given_name::text AS given_name,
    family_name::text AS family_name,
    sex::text AS sex,
    age_years::float AS age_years,
    age_months::float AS age_months,
    CASE
        WHEN age_years::float < 2 THEN '0-2 yrs'
        WHEN age_years::float BETWEEN 2 AND 5 THEN '2-5 yrs'
        WHEN age_years::float BETWEEN 5 AND 16 THEN '5-16 yrs'
        WHEN age_years::float > 16 THEN '16+ yrs'
        ELSE 'Unknown'
    END AS age_group,
    country::text AS country,
    county::text AS county,
    subcounty::text AS subcounty,
    town_village_camp::text AS town_village_camp,
    phone_number::text AS phone_number,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    title_residence::text AS title_residence,
    landmark::text AS landmark,
    title_parent_guradian::text AS title_parent_guradian,
    guardian_family_name::text AS guardian_family_name,
    guardian_given_name::text AS guardian_given_name,
    guardian_phone_number::text AS guardian_phone_number,
    title_respondent::text AS title_respondent,
    respondent::text AS respondent,
    respondent_family_name::text AS respondent_family_name,
    respondent_given_name::text AS respondent_given_name,
    respondent_address::text AS respondent_address,
    respondent_phone_number::text AS respondent_phone_number,
    respondent_relationship::text AS respondent_relationship,
    record_exposure::text AS record_exposure,
    exposure_type::text AS exposure_type,
    food_name::text AS food_name,
    food_consumption_date::text AS food_consumption_date,
    food_source::text AS food_source,
    food_participants_count::text AS food_participants_count,
    food_affected_participants_count::text AS food_affected_participants_count,
    water_sources::text AS water_sources,
    water_sources_other::text AS water_sources_other,
    toilet_types::text AS toilet_types,
    toilet_types_other::text AS toilet_types_other,
    travel_origin_country::text AS travel_origin_country,
    travel_origin_city::text AS travel_origin_city,
    travel_departure_date::text AS travel_departure_date,
    travel_destination_country::text AS travel_destination_country,
    travel_destination_city::text AS travel_destination_city,
    travel_arrival_date::text AS travel_arrival_date,
    animal_exposure::text AS animal_exposure,
    animal_exposure_other::text AS animal_exposure_other,
    animal_species::text AS animal_species,
    animal_species_other::text AS animal_species_other,
    animal_exposure_location::text AS animal_exposure_location,
    animal_exposure_date::text AS animal_exposure_date,
    type_of_care::text AS type_of_care,
    health_facility_mfl_number::text AS health_facility_mfl_number,
    '' AS health_facility_name,
    date_of_admission::text AS date_of_admission,
    inpatient_number::text AS inpatient_number,
    outcome_of_patient::text AS outcome_of_patient,
    date_of_death::text AS date_of_death,
    status_of_patient::text AS status_of_patient,
    '' AS date_of_discharge,    
    rdt_done::text AS rdt_done,
    rdt_results::text AS rdt_results,
    name_of_laboratory::text AS laboratory_name,
    date_of_sample_collection::text AS date_of_sample_collection,
    were_samples_collected::text AS were_samples_collected,
    laboratory_samples_collected::text AS laboratory_samples_collected,
    laboratory_samples_collected_others::text AS laboratory_samples_collected_others,
    vaccinated::text AS vaccinated,
    vaccine_doses::text AS vaccine_doses,
    vaccination_date::text AS vaccination_date,
    date_of_onset::text AS date_of_onset,
    symptoms::text AS symptoms,
    symptoms_other::text AS symptoms_other
FROM {{ ref('stg_diarrhoeal_disease') }}
WHERE date_of_onset IS NOT NULL

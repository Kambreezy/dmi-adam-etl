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
    location_of_investigation,
    given_name,
    family_name,
    sex,
    age_years,
    age_months,
    age_group,
    date_of_birth,
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
    respondent_demographics,
    respondent_family_name_demographics,
    respondent_given_name_demographics,
    respondent_phone_number_demographics,
    respondent_relationship_demographics,
    respondent_address_demographics,
    contact_surname,
    contact_given_name,
    contact_gender,
    contact_age,
    outcome_final_case_classification,
    outcome_final_patient_status,
    outcome_final_patient_status_other,
    outcome_reason_for_referral,
    outcome_date_of_outcome,
    exposure_record_exposure,
    exposure_exposure_type,
    exposure_food_name,
    exposure_food_consumption_date,
    exposure_food_source,
    exposure_food_participants_count,
    exposure_food_affected_participants_count,
    exposure_water_sources,
    exposure_water_sources_other,
    exposure_toilet_types,
    exposure_toilet_types_other,
    exposure_travel_origin_country,
    exposure_travel_origin_city,
    exposure_travel_departure_date,
    exposure_travel_destination_country,
    exposure_travel_destination_city,
    exposure_travel_arrival_date,
    exposure_animal_exposure,
    exposure_animal_exposure_other,
    exposure_animal_species,
    exposure_animal_species_other,
    exposure_animal_exposure_location,
    animal_exposure_date,
    respondent,
    respondent_family_name,
    respondent_given_name,
    respondent_phone_number,
    respondent_address,
    respondent_relationship,
    respondent_relationship_other,
    case_seen_at_facility,
    date_first_seen_at_facility,
    admitted,
    health_facility_name,
    admission_date,
    inpatient_number,
    discharge_date,
    patient_status,
    laboratory_sample_collected,
    laboratory_samples_collected,
    laboratory_samples_collected_others,
    sample_date,
    rdt_done,
    rdt_results,
    samples_sent_to_laboratory,
    laboratory_name,
    sample_sent_to_lab_date,
    symptoms,
    symptoms_other,
    symptoms_unexplained_bleeding,
    symptoms_start_date,
    title_symptoms_start_location,
    symptoms_start_county,
    symptoms_start_subcounty,
    symptoms_start_city_town_village_camp,
    outcome
FROM {{ ref('int_viral_hemorrhagic_fever') }}

SELECT
    (doc ->> 'mform_id')::text AS mform_id,
    (doc ->> 'fform_id')::text AS form_id,
    (doc ->> 'ident')::text AS case_unique_id,
    (doc ->> 'created_username')::text AS created_username,
    (doc ->> 'created_timestamp')::text AS created_timestamp,
    (doc ->> 'modified_username')::text AS modified_username,
    (doc ->> 'modified_timestamp')::text AS modified_timestamp,
    (doc -> 'location' ->> 'accuracy')::text AS location_accuracy,
    (doc -> 'location' ->> 'latitude')::text AS location_latitude,
    (doc -> 'location' ->> 'longitude')::text AS location_longitude,
    (doc -> 'DFields' -> 'values' -> 'syndrome' ->> 'df_value')::text AS syndrome,
    (doc -> 'DFields' -> 'values' -> 'disease' ->> 'df_value')::text AS disease,
    (doc -> 'DFields' -> 'values' -> 'EPID' ->> 'df_value')::text AS epid,
    (doc -> 'DFields' -> 'values' -> 'date_of_investigation' ->> 'df_value')::text AS date_of_investigation,
    (doc -> 'DFields' -> 'values' -> 'location_of_investigation' ->> 'df_value')::text AS location_of_investigation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'given' ->> 'df_value')::text AS given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'family' ->> 'df_value')::text AS family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'sex' ->> 'df_value')::text AS sex,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_years' ->> 'df_value')::text AS age_years,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_months' ->> 'df_value')::text AS age_months,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'landmark' ->> 'df_value')::text AS landmark,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_parent_guradian' ->> 'df_value')::text AS title_parent_guradian,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_family_name' ->> 'df_value')::text AS guardian_family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_given_name' ->> 'df_value')::text AS guardian_given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'guardian_phone_number' ->> 'df_value')::text AS guardian_phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_respondent' ->> 'df_value')::text AS title_respondent,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent' ->> 'df_value')::text AS respondent,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_family_name' ->> 'df_value')::text AS respondent_family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_given_name' ->> 'df_value')::text AS respondent_given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_address' ->> 'df_value')::text AS respondent_address,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_phone_number' ->> 'df_value')::text AS respondent_phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'respondent_relationship' ->> 'df_value')::text AS respondent_relationship,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'record_exposure' ->> 'df_value')::text AS record_exposure,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'exposure_type' ->> 'df_value')::text AS exposure_type,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_name' ->> 'df_value')::text AS food_name,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_consumption_date' ->> 'df_value')::text AS food_consumption_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_source' ->> 'df_value')::text AS food_source,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_participants_count' ->> 'df_value')::text AS food_participants_count,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'food_affected_participants_count' ->> 'df_value')::text AS food_affected_participants_count,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'water_sources' ->> 'df_value')::text AS water_sources,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'water_sources_other' ->> 'df_value')::text AS water_sources_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'toilet_types' ->> 'df_value')::text AS toilet_types,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'toilet_types_other' ->> 'df_value')::text AS toilet_types_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_country' ->> 'df_value')::text AS travel_origin_country,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_city' ->> 'df_value')::text AS travel_origin_city,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_departure_date' ->> 'df_value')::text AS travel_departure_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_country' ->> 'df_value')::text AS travel_destination_country,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_city' ->> 'df_value')::text AS travel_destination_city,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'travel_arrival_date' ->> 'df_value')::text AS travel_arrival_date,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure' ->> 'df_value')::text AS animal_exposure,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_other' ->> 'df_value')::text AS animal_exposure_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'animal_species' ->> 'df_value')::text AS animal_species,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'animal_species_other' ->> 'df_value')::text AS animal_species_other,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_location' ->> 'df_value')::text AS animal_exposure_location,
    (doc -> 'DForms' -> 'case_exposure_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_date' ->> 'df_value')::text AS animal_exposure_date,
    -- (doc -> 'DForms' -> 'clinical_care_generic' -> 0 -> 'DFields' -> 'values' -> 'health_facility_name' ->> 'df_value')::text AS health_facility_name,
    -- (doc -> 'DForms' -> 'clinical_care_generic' -> 0 -> 'DFields' -> 'values' -> 'date_of_discharge' ->> 'df_value')::text AS date_of_discharge,    
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'rdt_done' ->> 'df_value')::text AS rdt_done,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'rdt_results' ->> 'df_value')::text AS rdt_results,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'name_of_laboratory' ->> 'df_value')::text AS name_of_laboratory,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_sample_collection' ->> 'df_value')::text AS date_of_sample_collection,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'were_samples_collected' ->> 'df_value')::text AS were_samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'culture_done' ->> 'df_value')::text AS culture_done,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_culture_test' ->> 'df_value')::text AS date_of_culture_test,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'date_of_culture_result' ->> 'df_value')::text AS date_of_culture_result,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'culture_result_diarrhoeal' ->> 'df_value')::text AS culture_result_diarrhoeal,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected' ->> 'df_value')::text AS laboratory_samples_collected,
    (doc -> 'DForms' -> 'laboratory_information_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'laboratory_samples_collected_others' ->> 'df_value')::text AS laboratory_samples_collected_others,
    (doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'vaccinated' ->> 'df_value')::text AS vaccinated,
    (doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'vaccine_doses' ->> 'df_value')::text AS vaccine_doses,
    (doc -> 'DForms' -> 'vaccination_diarrhoeal_disease' -> 0 -> 'DFields' -> 'values' -> 'vaccination_date' ->> 'df_value')::text AS vaccination_date,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'symptoms' ->> 'df_value')::text AS symptoms,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'symptoms_other' ->> 'df_value')::text AS symptoms_other,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'level_of_hydration' ->> 'df_value')::text AS level_of_hydration,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'antibiotics_taken' ->> 'df_value')::text AS antibiotics_taken,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'antibiotic_name' ->> 'df_value')::text AS antibiotic_name,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'title_clinical_care' ->> 'df_value')::text AS title_clinical_care,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'health_facility_mfl_number' ->> 'df_value')::text AS health_facility_mfl_number,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'type_of_care' ->> 'df_value')::text AS type_of_care,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_admission' ->> 'df_value')::text AS date_of_admission,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'inpatient_number' ->> 'df_value')::text AS inpatient_number,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'outpatient_number' ->> 'df_value')::text AS outpatient_number,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'outcome_of_patient' ->> 'df_value')::text AS outcome_of_patient,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'date_of_death' ->> 'df_value')::text AS date_of_death,
    (doc -> 'DForms' -> 'clinical_information_diarrhoeal_diseases' -> 0 -> 'DFields' -> 'values' -> 'status_of_patient' ->> 'df_value')::text AS status_of_patient
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc -> 'DFields' -> 'values' -> 'syndrome' ->> 'df_value')::text = 'Diarrhoeal Disease'
AND
    (doc -> 'ident') IS NOT NULL

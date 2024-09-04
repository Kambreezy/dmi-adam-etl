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
    (doc -> 'DFields' -> 'values' -> 'type_of_case' ->> 'df_value')::text AS type_of_case,
    (doc -> 'DFields' -> 'values' -> 'unique_id_of_case' ->> 'df_value')::text AS unique_id_of_case,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'given' ->> 'df_value')::text AS given_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'family' ->> 'df_value')::text AS family_name,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'sex' ->> 'df_value')::text AS sex,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_years' ->> 'df_value')::text AS age_years,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'age_months' ->> 'df_value')::text AS age_months,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'date_of_birth' ->> 'df_value')::text AS date_of_birth,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'country' ->> 'df_value')::text AS country,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'county' ->> 'df_value')::text AS county,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'phone_number' ->> 'df_value')::text AS phone_number,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation' ->> 'df_value')::text AS occupation,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'occupation_other' ->> 'df_value')::text AS occupation_other,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'title_residence' ->> 'df_value')::text AS title_residence,
    (doc -> 'DForms' -> 'case_demographics' -> 0 -> 'DFields' -> 'values' -> 'town_village_camp' ->> 'df_value')::text AS town_village_camp,
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
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'date_of_onset' ->> 'df_value')::text AS date_of_onset,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'fever_at_onset_of_paralysis' ->> 'df_value')::text AS fever_at_onset_of_paralysis,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'progressive_paralysis' ->> 'df_value')::text AS progressive_paralysis,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'paralysis_acute_and_flaccid' ->> 'df_value')::text AS paralysis_acute_and_flaccid,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'paralysis_asymmetrical' ->> 'df_value')::text AS paralysis_asymmetrical,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'paralysis_site' ->> 'df_value')::text AS paralysis_site,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'paralysis_site_other' ->> 'df_value')::text AS paralysis_site_other,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'paralyzed_limb_sensitive_to_pain' ->> 'df_value')::text AS paralyzed_limb_sensitive_to_pain,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'injection_before_paralysis' ->> 'df_value')::text AS injection_before_paralysis,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'injection_site' ->> 'df_value')::text AS injection_site,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'provisial_diagnosis' ->> 'df_value')::text AS provisional_diagnosis,
    (doc -> 'DForms' -> 'case_investigation_afp' -> 0 -> 'DFields' -> 'values' -> 'outcome' ->> 'df_value')::text AS outcome,    
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'record_exposure' ->> 'df_value')::text AS record_exposure,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'exposure_type' ->> 'df_value')::text AS exposure_type,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_name' ->> 'df_value')::text AS food_name,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_consumption_date' ->> 'df_value')::text AS food_consumption_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_source' ->> 'df_value')::text AS food_source,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_participants_count' ->> 'df_value')::text AS food_participants_count,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'food_affected_participants_count' ->> 'df_value')::text AS food_affected_participants_count,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'water_sources' ->> 'df_value')::text AS water_sources,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'water_sources_other' ->> 'df_value')::text AS water_sources_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'toilet_types' ->> 'df_value')::text AS toilet_types,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'toilet_types_other' ->> 'df_value')::text AS toilet_types_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_country' ->> 'df_value')::text AS travel_origin_country,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_origin_city' ->> 'df_value')::text AS travel_origin_city,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_departure_date' ->> 'df_value')::text AS travel_departure_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_country' ->> 'df_value')::text AS travel_destination_country,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_destination_city' ->> 'df_value')::text AS travel_destination_city,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'travel_arrival_date' ->> 'df_value')::text AS travel_arrival_date,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure' ->> 'df_value')::text AS animal_exposure,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_other' ->> 'df_value')::text AS animal_exposure_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_species' ->> 'df_value')::text AS animal_species,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_species_other' ->> 'df_value')::text AS animal_species_other,
    (doc -> 'DForms' -> 'case_exposure' -> 0 -> 'DFields' -> 'values' -> 'animal_exposure_location' ->> 'df_value')::text AS animal_exposure_location,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'total_vaccine_doses' ->> 'df_value')::text AS total_vaccine_doses,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'date_of_opv_doses_at_birth' ->> 'df_value')::text AS date_of_opv_doses_at_birth,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'first_dose_date' ->> 'df_value')::text AS first_dose_date,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'second_dose_date' ->> 'df_value')::text AS second_dose_date,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'third_dose_date' ->> 'df_value')::text AS third_dose_date,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'fourth_dose_date' ->> 'df_value')::text AS fourth_dose_date,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'last_dose_date' ->> 'df_value')::text AS last_dose_date,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'last_opv_sia_dose' ->> 'df_value')::text AS last_opv_sia_dose,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'total_opv_sia_doses' ->> 'df_value')::text AS total_opv_sia_doses,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'total_opv_ri_doses' ->> 'df_value')::text AS total_opv_ri_doses,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'total_ipv_sia_doses' ->> 'df_value')::text AS total_ipv_sia_doses,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'total_ipv_ri_doses' ->> 'df_value')::text AS total_ipv_ri_doses,
    (doc -> 'DForms' -> 'vaccination_afp' -> 0 -> 'DFields' -> 'values' -> 'date_ipv_sia_ri' ->> 'df_value')::text AS date_ipv_sia_ri,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'stool_condition' ->> 'df_value')::text AS stool_condition,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_final_culture_result_available' ->> 'df_value')::text AS date_final_culture_result_available,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_specimen_received_in_ic_lab' ->> 'df_value')::text AS date_specimen_received_in_ic_lab,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'lab_id' ->> 'df_value')::text AS lab_id,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'final_cell_culture_result' ->> 'df_value')::text AS final_cell_culture_result,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_results_sent_to_national_epi' ->> 'df_value')::text AS date_results_sent_to_national_epi,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_isolate_sent_by_national_laboratory_to_regional_laboratory' ->> 'df_value')::text AS date_isolate_sent_by_national_laboratory_to_regional_laboratory,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_itd_results_sent_by_regional_laboratory' ->> 'df_value')::text AS date_itd_results_sent_by_regional_laboratory,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_itd_results_received_by_county' ->> 'df_value')::text AS date_itd_results_received_by_county,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_isolate_sent_for_sequencing' ->> 'df_value')::text AS date_isolate_sent_for_sequencing,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'date_final_results_sent_to_epi' ->> 'df_value')::text AS date_final_results_sent_to_epi,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'title_final_results' ->> 'df_value')::text AS title_final_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'w1_results' ->> 'df_value')::text AS w1_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'w2_results' ->> 'df_value')::text AS w2_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'w3_results' ->> 'df_value')::text AS w3_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'discordant_sabin' ->> 'df_value')::text AS discordant_sabin,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'sl1_results' ->> 'df_value')::text AS sl1_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'sl2_results' ->> 'df_value')::text AS sl2_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'sl3_results' ->> 'df_value')::text AS sl3_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'VDPV1_results' ->> 'df_value')::text AS vdpv1_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'VDPV2_results' ->> 'df_value')::text AS vdpv2_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'VDPV3_results' ->> 'df_value')::text AS vdpv3_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'RNEV_results' ->> 'df_value')::text AS rnev_results,
    (doc -> 'DForms' -> 'specimen_results_afp' -> 0 -> 'DFields' -> 'values' -> 'RNPENT_results' ->> 'df_value')::text AS rnpent_results,
    (doc -> 'DForms' -> 'specimen_collection_afp' -> 0 -> 'DFields' -> 'values' -> 'date_first_specimen_collected' ->> 'df_value')::text AS date_first_specimen_collected,
    (doc -> 'DForms' -> 'specimen_collection_afp' -> 0 -> 'DFields' -> 'values' -> 'date_second_specimen_collected' ->> 'df_value')::text AS date_second_specimen_collected,
    (doc -> 'DForms' -> 'specimen_collection_afp' -> 0 -> 'DFields' -> 'values' -> 'date_specimen_sent_to_national' ->> 'df_value')::text AS date_specimen_sent_to_national,
    (doc -> 'DForms' -> 'specimen_collection_afp' -> 0 -> 'DFields' -> 'values' -> 'date_specimen_received_at_national' ->> 'df_value')::text AS date_specimen_received_at_national,
    (doc -> 'DForms' -> 'specimen_collection_afp' -> 0 -> 'DFields' -> 'values' -> 'date_specimen_sent_to_lab' ->> 'df_value')::text AS date_specimen_sent_to_lab,
    (doc -> 'DForms' -> 'final_classification_afp' -> 0 -> 'DFields' -> 'values' -> 'immunocompromised_status_suspected' ->> 'df_value')::text AS immunocompromised_status_suspected,
    (doc -> 'DForms' -> 'final_classification_afp' -> 0 -> 'DFields' -> 'values' -> 'afp_final_case_classification' ->> 'df_value')::text AS afp_final_case_classification,
    (doc -> 'DForms' -> 'final_classification_afp' -> 0 -> 'DFields' -> 'values' -> 'afp_vdpv_serotype' ->> 'df_value')::text AS afp_vdpv_serotype,
    (doc -> 'DForms' -> 'patient_movements_afp' -> 0 -> 'DFields' -> 'values' -> 'place' ->> 'df_value')::text AS place,
    (doc -> 'DForms' -> 'patient_movements_afp' -> 0 -> 'DFields' -> 'values' -> 'duration_months' ->> 'df_value')::text AS duration_months,
    (doc -> 'DForms' -> 'patient_movements_afp' -> 0 -> 'DFields' -> 'values' -> 'duration_days' ->> 'df_value')::text AS duration_days,
    (doc -> 'DForms' -> 'vaccination_acute_flaccid_paralysis' -> 0 -> 'DFields' -> 'values' -> 'vaccine_administered' ->> 'df_value')::text AS vaccine_administered,
    (doc -> 'DForms' -> 'vaccination_acute_flaccid_paralysis' -> 0 -> 'DFields' -> 'values' -> 'date_of_vaccination' ->> 'df_value')::text AS date_of_vaccination,
    (doc -> 'DForms' -> 'vaccination_acute_flaccid_paralysis' -> 0 -> 'DFields' -> 'values' -> 'total_vaccine_doses' ->> 'df_value')::text AS total_vaccine_doses2,
    (doc -> 'DForms' -> 'vaccination_acute_flaccid_paralysis' -> 0 -> 'DFields' -> 'values' -> 'total_vaccine_sia_doses' ->> 'df_value')::text AS total_vaccine_sia_doses
FROM {{ source('couchdb', 'couchdb') }}
WHERE 
    (doc ->> 'type')::text = 'dform'
AND
    (doc -> 'DFields' -> 'values' -> 'disease' ->> 'df_value')::text = 'AFP'
AND
    (doc ->> 'ident') IS NOT NULL

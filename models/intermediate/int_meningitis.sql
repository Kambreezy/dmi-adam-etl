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
    CASE WHEN date_of_onset ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_onset, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_onset ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_onset, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
    epid::text AS epid,
    date_of_investigation::text AS date_of_investigation,
    type_of_case::text AS type_of_case,
    unique_id_of_case::text AS unique_id_of_case,
    given_name::text AS given_name,
    family_name::text AS family_name,
    sex::text AS sex,
    CASE 
        WHEN age_years ~ '^[0-9]+(\.[0-9]+)?$' THEN age_years::float
        ELSE NULL
    END AS age_years,
    CASE 
        WHEN age_months ~ '^[0-9]+(\.[0-9]+)?$' THEN age_months::float
        ELSE NULL
    END AS age_months,
    CASE
        WHEN NOT (age_years ~ '^[0-9]+(\.[0-9]+)?$') THEN 'Unknown'
        WHEN age_years::float < 2 THEN '0-2 yrs'
        WHEN age_years::float BETWEEN 2 AND 5 THEN '2-5 yrs'
        WHEN age_years::float BETWEEN 5 AND 16 THEN '5-16 yrs'
        WHEN age_years::float > 16 THEN '16+ yrs'
        ELSE 'Unknown'
    END AS age_group,
    date_of_birth::text AS date_of_birth,
    phone_number::text AS phone_number,
    occupation::text AS occupation,
    occupation_other::text AS occupation_other,
    country::text AS country,
    county::text AS county,
    subcounty::text AS subcounty,
    town_village_camp::text AS town_village_camp,
    landmark::text AS landmark,
    title_residence::text AS title_residence,
    title_parent_guardian::text AS title_parent_guardian,
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
    reporting_county::text AS reporting_county,
    reporting_subcounty::text AS reporting_subcounty,
    reporting_health_facility::text AS reporting_health_facility,
    vaccine::text AS vaccine,
    vaccine_doses::text AS vaccine_doses,
    vaccine_last_dose_date::text AS vaccine_last_dose_date,
    vaccine_information_source::text AS vaccine_information_source,
    lumbar_puncture_date::text AS lumbar_puncture_date,
    lumbar_puncture_time::text AS lumbar_puncture_time,
    celebrospinal_fluid_appearance::text AS celebrospinal_fluid_appearance,
    inoculation_date::text AS inoculation_date,
    inoculation_time::text AS inoculation_time,
    sample_transport_medium::text AS sample_transport_medium,
    sample_transport_medium_others::text AS sample_transport_medium_others,
    laboratory_test_requested::text AS laboratory_test_requested,
    reference_laboratory::text AS reference_laboratory,
    patient_on_treatment::text AS patient_on_treatment,
    symptoms::text AS symptoms,
    date_of_onset::text AS date_of_onset,
    clinical_health_facility::text AS clinical_health_facility,
    date_first_seen_at_facility::text AS date_first_seen_at_facility,
    admission_date::text AS admission_date,
    patient_status::text AS patient_status
FROM {{ ref('stg_meningitis') }}
WHERE date_of_onset IS NOT NULL
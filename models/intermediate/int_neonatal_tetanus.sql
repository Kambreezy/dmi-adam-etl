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
    ''::text AS syndrome,
    disease::text AS disease,
    CASE WHEN date_of_investigation ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_investigation, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_investigation ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_investigation, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
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
    CASE WHEN date_of_birth ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_birth, 'DD/MM/YYYY')::date ELSE NULL END AS date_of_birth,
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
    delivery_location::text AS delivery_location,
    sterile_code_cutter::text AS sterile_code_cutter,
    cord_stump_treated::text AS cord_stump_treated,
    baby_age_days::text AS baby_age_days,
    baby_suckling_normally::text AS baby_suckling_normally,
    confirmed_nnt::text AS confirmed_nnt,
    baby_treated_at_facility::text AS baby_treated_at_facility,
    baby_mother_alive::text AS baby_mother_alive,
    title_clinical_care::text AS title_clinical_care,
    type_of_care::text AS type_of_care,
    date_of_admission::text AS date_of_admission,
    inpatient_number::text AS inpatient_number,
    name_of_health_facility::text AS name_of_health_facility,
    mfl_number_of_health_facility::text AS mfl_number_of_health_facility,
    outcome_of_patient::text AS outcome_of_patient,
    status_of_patient::text AS status_of_patient,
    date_of_discharge::text AS date_of_discharge,
    date_of_death::text AS date_of_death,
    laboratory_samples_were_collected::text AS laboratory_samples_were_collected,
    laboratory_samples_collected::text AS laboratory_samples_collected,
    laboratory_samples_collected_other::text AS laboratory_samples_collected_other,
    date_of_sample_collection::text AS date_of_sample_collection,
    result_of_laboratory_test::text AS result_of_laboratory_test,
    date_of_laboratory_results::text AS date_of_laboratory_results,
    title_laboratory_facility::text AS title_laboratory_facility,
    name_of_laboratory_facility::text AS name_of_laboratory_facility,
    mfl_number_of_laboratory_facility::text AS mfl_number_of_laboratory_facility
FROM {{ ref('stg_neonatal_tetanus') }}
WHERE date_of_investigation IS NOT NULL

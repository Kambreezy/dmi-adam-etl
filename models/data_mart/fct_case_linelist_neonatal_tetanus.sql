{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['case_date']},
      {'columns': ['epi_week']},
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
    date_of_birth,
    phone_number,
    occupation,
    occupation_other,
    country,
    county,
    subcounty,
    town_village_camp,
    landmark,
    title_residence,
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
    delivery_location,
    sterile_code_cutter,
    cord_stump_treated,
    baby_age_days,
    baby_suckling_normally,
    confirmed_nnt,
    baby_treated_at_facility,
    baby_mother_alive,
    title_clinical_care,
    type_of_care,
    date_of_admission,
    inpatient_number,
    name_of_health_facility,
    mfl_number_of_health_facility,
    outcome_of_patient,
    status_of_patient,
    date_of_discharge,
    date_of_death,
    laboratory_samples_were_collected,
    laboratory_samples_collected,
    laboratory_samples_collected_other,
    date_of_sample_collection,
    result_of_laboratory_test,
    date_of_laboratory_results,
    title_laboratory_facility,
    name_of_laboratory_facility,
    mfl_number_of_laboratory_facility
FROM {{ ref('int_neonatal_tetanus') }}

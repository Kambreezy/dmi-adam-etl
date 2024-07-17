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
    (doc -> 'DFields' -> 'values' -> 'village' ->> 'df_value')::text AS village,
    (doc -> 'DFields' -> 'values' -> 'sublocation_name' ->> 'df_value')::text AS sublocation_name,
    (doc -> 'DFields' -> 'values' -> 'commuity_unit' ->> 'df_value')::text AS commuity_unit,
    (doc -> 'DFields' -> 'values' -> 'location_name' ->> 'df_value')::text AS location_name,
    (doc -> 'DFields' -> 'values' -> 'ward_name' ->> 'df_value')::text AS ward_name,
    (doc -> 'DFields' -> 'values' -> 'subcounty' ->> 'df_value')::text AS subcounty,
    (doc -> 'DFields' -> 'values' -> 'clts_triggering_date' ->> 'df_value')::text AS clts_triggering_date,
    (doc -> 'DFields' -> 'values' -> 'clts_hcw_name' ->> 'df_value')::text AS clts_hcw_name,
    (doc -> 'DFields' -> 'values' -> 'clts_hcw_contact' ->> 'df_value')::text AS clts_hcw_contact,
    (doc -> 'DFields' -> 'values' -> 'clts_natural_leader_name' ->> 'df_value')::text AS clts_natural_leader_name,
    (doc -> 'DFields' -> 'values' -> 'clts_natural_leader_phone_number' ->> 'df_value')::text AS clts_natural_leader_phone_number,
    (doc -> 'DFields' -> 'values' -> 'household_head_name' ->> 'df_value')::text AS household_head_name,
    (doc -> 'DFields' -> 'values' -> 'clts_number_of_people' ->> 'df_value')::text AS clts_number_of_people,
    (doc -> 'DFields' -> 'values' -> 'clts_male_number' ->> 'df_value')::text AS clts_male_number,
    (doc -> 'DFields' -> 'values' -> 'clts_female_number' ->> 'df_value')::text AS clts_female_number,
    (doc -> 'DFields' -> 'values' -> 'clts_children_number' ->> 'df_value')::text AS clts_children_number,
    (doc -> 'DFields' -> 'values' -> 'clts_at_trigger' ->> 'df_value')::text AS clts_at_trigger,
    (doc -> 'DFields' -> 'values' -> 'clts_household_latrine_before_trigger' ->> 'df_value')::text AS clts_household_latrine_before_trigger,
    (doc -> 'DFields' -> 'values' -> 'clts_household_handwashing_before_trigger' ->> 'df_value')::text AS clts_household_handwashing_before_trigger,
    (doc -> 'DFields' -> 'values' -> 'clts_commitments' ->> 'df_value')::text AS clts_commitments,
    (doc -> 'DFields' -> 'values' -> 'clts_commitment_latrine_construction_date' ->> 'df_value')::text AS clts_commitment_latrine_construction_date,
    (doc -> 'DFields' -> 'values' -> 'clts_commitment_latrine_under_construction' ->> 'df_value')::text AS clts_commitment_latrine_under_construction,
    (doc -> 'DFields' -> 'values' -> 'clts_commitment_latrine_completion_date' ->> 'df_value') AS clts_commitment_latrine_completion_date,
    (doc -> 'DFields' -> 'values' -> 'title_clts_follow_up' ->> 'df_value')::text AS title_clts_follow_up,
    (doc -> 'DFields' -> 'values' -> 'clts_commitment_new_latrine_constructed' ->> 'df_value')::text AS clts_commitment_new_latrine_constructed,
    (doc -> 'DFields' -> 'values' -> 'ctls_follow_up_handwashing_facility_constructed' ->> 'df_value')::text AS ctls_follow_up_handwashing_facility_constructed,
    (doc -> 'DFields' -> 'values' -> 'clts_followup_latrine_construction_date' ->> 'df_value') AS clts_followup_latrine_construction_date
FROM {{ source('couchdb', 'couchdb') }}
WHERE
    (doc ->> 'type')::text = 'dform'
AND
    (doc -> 'DFields' -> 'values' -> 'village' ->> 'df_value')::text IS NOT NULL
AND
    (doc -> 'ident') IS NOT NULL

SELECT
    case_unique_id::text AS case_unique_id,
    CASE WHEN date_of_sample_collection ~ '^\d{2}/\d{2}/\d{4}$' THEN to_timestamp(date_of_sample_collection, 'DD/MM/YYYY')::date ELSE NULL END AS case_date,
    CASE WHEN date_of_sample_collection ~ '^\d{2}/\d{2}/\d{4}$' THEN to_char(to_timestamp(date_of_sample_collection, 'DD/MM/YYYY'), 'YYYY "W"IW') ELSE NULL END AS epi_week,
    created_username::text AS created_username,
    created_timestamp::text AS created_timestamp,
    modified_username::text AS modified_username,
    modified_timestamp::text AS modified_timestamp,
    location_accuracy::text AS location_accuracy,
    location_latitude::text AS location_latitude,
    location_longitude::text AS location_longitude,
    sample_number::text AS sample_number,
    date_of_sample_collection::text AS date_of_sample_collection,
    time_of_sample_collection::text AS time_of_sample_collection,
    CASE
        WHEN TRIM(type_of_food_product::text) = '' THEN 'Unknown'
        WHEN type_of_food_product IS NULL THEN 'Unknown'
        ELSE TRIM(type_of_food_product::text)
    END AS type_of_food_product,
    CASE
        WHEN TRIM(type_of_food_sample::text) = '' THEN 'Unknown'
        WHEN type_of_food_sample IS NULL THEN 'Unknown'
        ELSE TRIM(type_of_food_sample::text)
    END AS type_of_food_sample,
    name_of_product::text AS name_of_product,
    description_of_product::text AS description_of_product,
    batch_number::text AS batch_number,
    date_of_manufacture::text AS date_of_manufacture,
    date_of_expiry::text AS date_of_expiry,
    name_of_manufacturer::text AS name_of_manufacturer,
    name_of_importer_dealer::text AS name_of_importer_dealer,
    method_of_collection::text AS method_of_collection,
    size_of_lot_sample::text AS size_of_lot_sample,
    specimen_seal_used::text AS specimen_seal_used,
    reason_for_collection::text AS reason_for_collection,
    reason_for_collection_other::text AS reason_for_collection_other,
    mode_of_sample_delivery::text AS mode_of_sample_delivery,
    date_of_dispatch::text AS date_of_dispatch,
    time_of_dispatch::text AS time_of_dispatch,
    number_of_invoice::text AS number_of_invoice,
    date_of_invoice::text AS date_of_invoice,
    shipping_record::text AS shipping_record,
    date_of_shipping::text AS date_of_shipping,
    sample_collector_name::text AS sample_collector_name,
    sample_collector_designation::text AS sample_collector_designation,
    collector_phone_number::text AS collector_phone_number,
    collectors_email_address::text AS collectors_email_address,
    title_site_information::text AS title_site_information,
    county::text AS county,
    subcounty::text AS subcounty,
    ward::text AS ward,
    landmark::text AS landmark,
    CASE
        WHEN TRIM(type_of_premise::text) = '' THEN 'Unknown'
        WHEN type_of_premise IS NULL THEN 'Unknown'
        ELSE TRIM(type_of_premise::text)
    END AS type_of_premise,
    name_of_premise::text AS name_of_premise,
    premise_phone_number::text AS premise_phone_number,
    premise_email_address::text AS premise_email_address
FROM {{ ref('stg_food_drugs_and_chemicals_sampling_form') }}
WHERE date_of_sample_collection IS NOT NULL

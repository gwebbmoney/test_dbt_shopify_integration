WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'CUSTOMER_ADDRESS') }}
)
SELECT id,
    customer_id,
    first_name,
    last_name,
    name,
    phone,
    address_1,
    address_2,
    city,
    country,
    country_code,
    province,
    province_code,
    zip,
    latitude,
    longitude,
    is_default,
    _fivetran_synced
FROM source




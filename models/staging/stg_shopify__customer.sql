WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'CUSTOMER') }}
)
SELECT id,
    first_name,
    last_name,
    email,
    phone,
    verified_email,
    email_marketing_consent_state,
    email_marketing_consent_opt_in_level,
    email_marketing_consent_consent_updated_at,
    metafield,
    created_at,
    updated_at,
    _fivetran_deleted,
    _fivetran_synced
FROM source



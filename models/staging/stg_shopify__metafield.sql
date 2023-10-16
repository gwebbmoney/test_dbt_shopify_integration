WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'METAFIELD') }}
)
SELECT id,
    owner_id,
    namespace,
    owner_resource,
    type,
    key,
    value,
    created_at,
    updated_at
FROM source 

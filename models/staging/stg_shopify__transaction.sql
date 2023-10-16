WITH source AS(
    SELECT * FROM {{source('shopify_raw', 'TRANSACTION')}}
)
SELECT id,
    order_id,
    kind,
    amount,
    created_at,
    processed_at,
    device_id,
    status,
    test,
    _fivetran_synced
FROM source
WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }}
)
SELECT order_id,
    index,
    code,
    type,
    amount,
    _fivetran_synced
FROM source






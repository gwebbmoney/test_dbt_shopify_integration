WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_ADJUSTMENT') }}
)
SELECT id,
    order_id,
    refund_id,
    amount,
    tax_amount,
    (amount + tax_amount) AS total_order_adjustment,
    kind,
    reason,
    _fivetran_synced
FROM source




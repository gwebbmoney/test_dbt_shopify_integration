WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_LINE_REFUND') }}
)
SELECT id,
    order_line_id,
    refund_id,
    restock_type,
    quantity,
    subtotal,
    total_tax,
    (subtotal + total_tax) AS line_item_refund,
    _fivetran_synced
FROM source



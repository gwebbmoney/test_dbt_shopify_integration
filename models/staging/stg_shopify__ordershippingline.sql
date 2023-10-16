WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_SHIPPING_LINE') }}
)
SELECT id,
    order_id,
    code,
    price,
    title,
    carrier_identifier,
    phone,
    delivery_category,
    discounted_price
FROM source






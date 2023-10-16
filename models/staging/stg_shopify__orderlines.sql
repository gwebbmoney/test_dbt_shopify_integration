WITH source AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_LINE')}}
)
SELECT id,
    order_id,
    product_id,
    variant_id,
    title,
    vendor,
    price,
    quantity,
    grams,
    sku,
    fulfillable_quantity,
    gift_card,
    requires_shipping,
    variant_title,
    properties,
    index,
    total_discount,
    pre_tax_price,
    fulfillment_status
FROM source




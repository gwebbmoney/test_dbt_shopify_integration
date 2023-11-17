WITH order_lines AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_LINE') }}
)
SELECT id AS order_line_id,
    order_id,
    product_id,
    variant_id, 
    title AS product_name,
    variant_title AS product_variant_name,
    sku,
    properties,
    index AS order_line,
    price*100 AS price_cents,
    quantity AS quantity_ordered,
    fulfillable_quantity,
    (price_cents * quantity) AS line_item_price_cents,
    total_discount*100 AS total_discount_cents,
    pre_tax_price*100 AS pre_tax_price_cents,
    gift_card
FROM order_lines -- ol LEFT JOIN {{ source('shopify_raw', 'PRODUCT_TAG') }} pt ON ol.product_id = pt.product_id
WITH order_lines AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_LINE') }}
)
SELECT id AS order_line_id,
    order_id,
    product_id AS shopify_product_id,
    (CASE
        WHEN p.shopify_product_id = ol.product_id THEN p.emma_product_id
        WHEN b.shopify_bundle_id = ol.product_id THEN b.emma_bundle_id
    END) AS emma_product_id,
    variant_id AS product_variant_id, 
    title AS product_name,
    variant_title AS product_variant_name,
    ol.sku,
    nullif(properties, []) AS bundle_properties,
    index AS order_line,
    (CASE
        WHEN p.shopify_product_id = ol.product_id THEN 'Product'
        WHEN b.shopify_bundle_id = ol.product_id THEN 'Bundle'
    END) AS skuable_type,
    bundle_type,
    p.product_tag,
    ol.price*100 AS price_cents,
    quantity AS quantity_ordered,
    fulfillable_quantity,
    (price_cents * quantity) AS line_item_price_cents,
    total_discount*100 AS total_discount_cents,
    pre_tax_price*100 AS pre_tax_price_cents,
    gift_card
FROM order_lines ol LEFT JOIN {{ ref("redaspen_product_variants") }} p ON ol.sku = p.sku AND ol.variant_id = p.shopify_product_variant_id -- Change to just sku join once duplicate products are eliminated
    LEFT JOIN {{ ref("redaspen_bundle_variants") }} b ON ol.sku = b.sku AND ol.variant_id = b.shopify_bundle_variant_id






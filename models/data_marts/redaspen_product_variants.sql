WITH products AS(
    SELECT * FROM {{ ref("int_shopify__products") }}
),
product_variants AS(
    SELECT pv.id AS product_variant_id,
    pv.title AS product_variant_title,
    p.product_id,
    p.product_title,
    p.product_type,
    pv.price,
    pv.sku,
    pv.position,
    pv.created_at,
    pv.updated_at,
    pv.grams,
    pv.weight,
    pv.inventory_item_id,
    pv.option_1,
    pv.option_2,
    pv.option_3
    FROM products p JOIN {{ source('shopify_raw', 'PRODUCT_VARIANT') }} pv ON p.id = pv.product_id
)
SELECT pv.*
FROM product_variants pv LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON pv.product_id = bv.bundle_id
WHERE bv.bundle_id IS NULL






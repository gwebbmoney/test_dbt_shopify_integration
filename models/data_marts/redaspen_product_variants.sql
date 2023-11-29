WITH products AS(
    SELECT * FROM {{ ref("int_shopify__products") }}
),
product_variants AS(
    SELECT pv.id AS product_variant_id,
    pv.title AS product_variant_title,
    p.product_id,
    p.emma_product_id,
    p.product_title,
    p.product_type,
    pv.sku
    FROM products p JOIN {{ source('shopify_raw', 'PRODUCT_VARIANT') }} pv ON p.product_id = pv.product_id
        LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON p.product_id = bv.bundle_id
    WHERE bv.bundle_id IS NULL
)
WITH full_join AS(SELECT COALESCE(p.product_id, pv.emma_product_id)::number AS emma_product_id,
                pv.product_id AS shopify_product_id,
                COALESCE(p.product_title, pv.product_title) AS product_title,
                pv.product_variant_id AS shopify_product_variant_id,
                pv.product_variant_title AS shopify_product_variant_title,
                COALESCE(p.sku, pv.sku) AS sku,
                p.category_id,
                COALESCE(p.product_type, pv.product_type) AS category_name,
                p.sub_category_id,
                p.sub_category_name
FROM product_variants pv FULL OUTER JOIN {{ ref("int_infotrax__products") }} p ON pv.sku = p.sku
WHERE p.skuable_type = 'Product'






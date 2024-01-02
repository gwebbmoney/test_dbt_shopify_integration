{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'products')}}

WITH products AS(
    SELECT * FROM {{ ref("int_shopify__products") }}
),
product_tag AS(
    SELECT product_id,  
        value AS product_tag
    FROM {{ source('shopify_raw', 'PRODUCT_TAG') }}
    WHERE value = 'bogos-gift' 
),
product_variants AS(
    SELECT pv.id AS product_variant_id,
    pv.title AS product_variant_title,
    p.product_id,
    p.emma_product_id,
    p.product_title,
    p.product_type,
    p.seasonality,
    p.collection,
    p.style_id,
    p.style,
    p.length_id,
    p.length,
    p.shape_id,
    p.shape,
    p.design,
    p.volume_id,
    p.volume,
    p.status_id,
    p.status,
    pv.sku
    FROM products p JOIN {{ source('shopify_raw', 'PRODUCT_VARIANT') }} pv ON p.product_id = pv.product_id
        LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON p.product_id = bv.shopify_bundle_id
    WHERE bv.shopify_bundle_id IS NULL
        AND bv.emma_bundle_id IS NULL
)
SELECT CAST(COALESCE(p.product_id, pv.emma_product_id) AS number) AS emma_product_id,
    pv.product_id AS shopify_product_id,
    (CASE 
        WHEN pt.product_tag = 'bogos-gift' THEN pv.product_title
        ELSE COALESCE(p.product_title, pv.product_title)
    END) AS product_title,
    pv.product_variant_id AS shopify_product_variant_id,
    pv.product_variant_title AS shopify_product_variant_title,
    COALESCE(p.sku, pv.sku) AS sku,
    p.category_id,
    COALESCE(p.product_type, pv.product_type) AS category_name,
    p.sub_category_id,
    p.sub_category_name,
    pt.product_tag,
    pv.seasonality,
    pv.collection,
    pv.style_id,
    pv.style,
    pv.length_id,
    pv.length,
    pv.shape_id,
    pv.shape,
    pv.design,
    pv.volume_id,
    pv.volume,
    pv.status_id, 
    pv.status
FROM product_variants pv FULL OUTER JOIN {{ ref("int_infotrax__products") }} p ON pv.sku = p.sku
    LEFT JOIN product_tag pt ON pv.product_id = pt.product_id
WHERE p.skuable_type = 'Product'





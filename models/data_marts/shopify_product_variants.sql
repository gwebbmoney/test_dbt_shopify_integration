{{ config(database = 'redaspen') }}

{{ config(schema = 'products')}}

-- Creates Transient Table within Snowflake that houses both Shopify and EMMA products
WITH products AS(
    SELECT * FROM {{ ref("int_shopify__products") }}
-- Grabs products from Shopify
),
product_tag AS(
    SELECT product_id,  
        value AS product_tag
    FROM {{ source('shopify_raw', 'PRODUCT_TAG') }}
    WHERE value = 'bogos-gift' 
-- Grabs product tag where value = 'bogos-gift'
-- This means 'Buy One Get One' product
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
        LEFT JOIN {{ ref("shopify_bundle_variants") }} bv ON p.product_id = bv.shopify_bundle_id
    WHERE bv.shopify_bundle_id IS NULL
        AND bv.emma_id IS NULL
-- Finds all product variants outside of bundles
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
    p.price,
    p.pv,
    p.component
FROM product_variants pv FULL OUTER JOIN {{ ref("int_infotrax__products") }} p ON pv.sku = p.sku
    LEFT JOIN product_tag pt ON pv.product_id = pt.product_id
WHERE p.skuable_type = 'Product'
-- Combines both EMMA and Shopify products into one table
-- Organizes table into it's final format





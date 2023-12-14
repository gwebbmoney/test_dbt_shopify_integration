{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'bundles')}}

WITH bundle_tag AS(
    SELECT *
    FROM {{ source('shopify_raw', 'PRODUCT_TAG') }}
),
bundle_type_tag AS(
SELECT product_id,
    value
FROM bundle_tag
WHERE value = 'Bundle_Fixed'
    OR value = 'Bundle_Custom'
),
bundles AS(
    SELECT p.*
    FROM {{ ref("int_shopify__products") }} p JOIN bundle_tag bt ON p.product_id = bt.product_id
    WHERE bt.value = 'Bundle'
),
bundle_variants AS(
    SELECT bv.id AS bundle_variant_id,
    bv.title AS bundle_variant_title,
    b.product_id AS bundle_id,
    b.emma_product_id AS emma_bundle_id,
    b.product_title AS bundle_title,
    btt.value AS bundle_type,
    bv.sku
    FROM {{ source('shopify_raw', 'PRODUCT_VARIANT') }} bv JOIN bundles b ON bv.product_id = b.product_id
        LEFT JOIN bundle_type_tag btt ON b.product_id = btt.product_id
)
SELECT CAST(COALESCE(b.product_id, bv.emma_bundle_id) AS number) AS emma_bundle_id,
    bv.bundle_id AS shopify_bundle_id,
    COALESCE(b.product_title, bv.bundle_title) AS bundle_title,
    bv.bundle_title AS shopify_bundle_title,
    bv.bundle_variant_id AS shopify_bundle_variant_id,
    bv.bundle_variant_title AS shopify_bundle_variant_title,
    COALESCE(b.sku, bv.sku) AS sku,
    COALESCE(bundle_type, skuable_type) AS bundle_type
FROM bundle_variants bv FULL OUTER JOIN {{ ref('int_infotrax__products') }} b ON bv.sku = b.sku
WHERE bundle_type IN ('Bundle_Custom', 'Bundle_Fixed')
    OR skuable_type = 'Bundle'
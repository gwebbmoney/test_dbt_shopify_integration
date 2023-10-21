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
    SELECT id,
        product_id,
        inventory_item_id,
        title,
        price,
        sku,
        position,
        created_at,
        updated_at,
        grams,
        weight,
        option_1,
        option_2,
        option_3
    FROM {{ source('shopify_raw', 'PRODUCT_VARIANT') }}
)
SELECT bv.id AS bundle_variant_id,
    bv.title AS bundle_variant_title,
    b.product_id AS bundle_id,
    b.product_title AS bundle_title,
    btt.value AS bundle_type,
    bv.price,
    bv.sku,
    bv.position,
    bv.created_at,
    bv.updated_at,
    bv.grams,
    bv.weight,
    bv.inventory_item_id,
    bv.option_1,
    bv.option_2,
    bv.option_3
FROM bundles b LEFT JOIN bundle_type_tag btt ON b.product_id = btt.product_id
    JOIN bundle_variants bv ON b.product_id = bv.product_id
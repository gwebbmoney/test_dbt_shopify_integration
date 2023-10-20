WITH products AS(
    SELECT * FROM {{ ref("int_shopify__products") }}
),
product_variant AS(
    SELECT id AS product_variant_id,
        product_id,
        inventory_item_id,
        title AS product_variant_title,
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
SELECT pv.product_variant_id,
    p.product_variant_title,
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
FROM products p JOIN product_variant pv ON p.product_id = pv.product_id







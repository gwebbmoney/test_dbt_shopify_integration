-- Creates a view that grabs all products that are within our Shopify system
-- NOTE: EMMA is our in-house application that houses various company data. For this purpose, we grab product information from this resource and attach it to the order lines table
WITH products AS(
    SELECT id AS product_id,
        title AS product_title,
        product_type,
        status,
        created_at,
        updated_at,
        published_at
    FROM {{ source('shopify_raw', 'PRODUCT') }}
    WHERE _fivetran_deleted = FALSE
-- Grabs Shopify products in the SHOPIFY.PRODUCT table
),
product_metafield_emma_id AS(
    SELECT owner_id,
        key,
        value
    FROM {{ source('shopify_raw', 'METAFIELD') }}
    WHERE owner_resource IN ('product', 'PRODUCT')
        AND key = 'emma_id'
-- Grabs the EMMA product id for each product
),
product_metafield_sub_category AS(
    SELECT owner_id,
        key,
        value
    FROM {{ source('shopify_raw', 'METAFIELD') }}
    WHERE owner_resource IN ('product', 'PRODUCT')
        AND key = 'product_sub_category'
-- Grabs the EMMA sub category id for each product
)
SELECT product_id,
    product_title,
    product_type,
    status,
    created_at,
    updated_at,
    published_at,
    (CASE WHEN pm.key = 'emma_id' THEN pm.value END) AS emma_product_id,
    (CASE WHEN pmsc.key = 'product_sub_category' THEN pmsc.value END) AS sub_category_name
FROM products p LEFT JOIN product_metafield_emma_id pm ON p.product_id = pm.owner_id
    LEFT JOIN product_metafield_sub_category pmsc ON p.product_id = pmsc.owner_id
-- Organizes product information from Shopify


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
),
product_metafield AS(
    SELECT owner_id,
        key,
        value
    FROM {{ source('shopify_raw', 'METAFIELD') }}
    WHERE owner_resource IN ('product', 'PRODUCT')
        AND key = 'emma_id'
)
SELECT product_id,
    product_title,
    product_type,
    status,
    created_at,
    updated_at,
    published_at,
    (CASE WHEN pm.key = 'emma_id' THEN pm.value END) AS emma_product_id
FROM products p LEFT JOIN product_metafield pm ON p.product_id = pm.owner_id












-- Creates view of all products and bundles shown within EMMA
-- NOTE: EMMA is our in-house application that houses various company data. For this purpose, we grab product information from this resource and attach it to the order lines table
SELECT p.id AS product_id,
    s.name AS sku,
    p.name AS product_title,
    c.id AS category_id,
    c.name AS product_type,
    sc.id AS sub_category_id,
    sc.name AS sub_category_name,
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
    p.finish,
    p.price,
    p.pv,
    p.component,
    NULL AS value,
    s.skuable_type
FROM {{ source("normalized_snowflake", 'PRODUCTS') }} p LEFT JOIN {{ source("redaspen", 'SKUS') }} s ON p.id = s.skuable_id
    LEFT JOIN {{ source("redaspen", 'CATEGORIES') }} c ON p.category_id = c.id
    LEFT JOIN {{ source("redaspen", 'SUB_CATEGORIES') }} sc ON p.sub_category_id = sc.id
WHERE s.skuable_type = 'Product'
-- Grabs all products within EMMA
UNION
SELECT b.id AS bundle_id,
    s.name AS sku,
    b.name AS bundle_title,
    NULL AS category_id,
    NULL AS product_type,
    NULL AS sub_category_id,
    NULL AS sub_category_name,
    NULL AS seasonality,
    NULL AS collection,
    NULL AS style_id,
    NULL AS style,
    NULL AS length_id,
    NULL AS length,
    NULL AS shape_id,
    NULL AS shape,
    NULL AS design,
    NULL AS volume_id,
    NULL AS volume,
    b.status_id AS status_id,
    NULL AS status,
    NULL AS finish,
    b.price AS price,
    b.pv AS pv,
    NULL AS component,
    b.value AS value,
    s.skuable_type
FROM {{ source("redaspen", 'BUNDLES') }} b LEFT JOIN {{ source("redaspen", 'SKUS') }} s ON b.id = s.skuable_id
WHERE s.skuable_type = 'Bundle'
-- Grabs all bundles within EMMA




SELECT p.id AS product_id,
    s.name AS sku,
    p.name AS product_title,
    c.id AS category_id,
    c.name AS product_type,
    sc.id AS sub_category_id,
    sc.name AS sub_category_name,
    s.skuable_type
FROM {{ source("redaspen", 'PRODUCTS') }} p LEFT JOIN {{ source("redaspen", 'SKUS') }} s ON p.id = s.skuable_id
    LEFT JOIN {{ source("redaspen", 'CATEGORIES') }} c ON p.category_id = c.id
    LEFT JOIN {{ source("redaspen", 'SUB_CATEGORIES') }} sc ON p.sub_category_id = sc.id
WHERE s.skuable_type = 'Product'
UNION
SELECT b.id AS bundle_id,
    s.name AS sku,
    b.name AS bundle_title,
    NULL AS category_id,
    NULL AS product_type,
    NULL AS sub_category_id,
    NULL AS sub_category_name,
    s.skuable_type
FROM {{ source("redaspen", 'BUNDLES') }} b LEFT JOIN {{ source("redaspen", 'SKUS') }} s ON b.id = s.skuable_id
WHERE s.skuable_type = 'Bundle'




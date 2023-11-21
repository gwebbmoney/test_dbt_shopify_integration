WITH products AS(
    SELECT p.product_id AS shopify_product_id,
        ip.product_id AS emma_product_id,
        (CASE
            WHEN p.product_title IS NULL THEN ip.product_title
            ELSE p.product_title
        END) AS product_title,
        (CASE
            WHEN p.product_type IS NULL THEN ip.product_type
            ELSE p.product_type
        END) AS category_name,
        ip.category_id,
        ip.sku,
        ip.skuable_type
    FROM {{ ref("int_shopify__products") }} p RIGHT JOIN {{ ref("int_infotrax_products") }} ip ON p.emma_product_id = ip.product_id
)
SELECT pv.*
FROM product_variants pv LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON pv.product_id = bv.bundle_id
WHERE bv.bundle_id IS NULL
    AND ip.skuable_type = 'Product'
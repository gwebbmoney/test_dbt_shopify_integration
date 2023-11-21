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
    FROM {{ ref("int_shopify__products") }} p RIGHT JOIN {{ ref("int_infotrax__products") }} ip ON p.emma_product_id = ip.product_id
)
SELECT p.*
FROM product p LEFT JOIN {{ ref("redaspen_bundles") }} b ON p.shopify_product_id = b.shopify_bundle_id
WHERE b.shopify_bundle_id IS NULL
    AND p.skuable_type = 'Product'










WITH bundle_tag AS(
    SELECT *
    FROM {{ source('shopify_raw', 'PRODUCT_TAG') }}
    WHERE value = 'Bundle'
),
bundles AS(
    SELECT p.product_id AS shopify_bundle_id,
        ip.product_id AS emma_bundle_id,
        (CASE
            WHEN p.product_title IS NULL THEN ip.product_title
            ELSE p.product_title
        END) AS bundle_title,
        ip.sku,
        ip.skuable_type,
        bt.value AS product_tag_value
    FROM {{ ref("int_shopify__products") }} p JOIN bundle_tag bt ON p.product_id = bt.product_id
       RIGHT JOIN {{ ref("int_infotrax__products") }} ip ON p.emma_product_id = ip.product_id
)
SELECT *
FROM bundles
WHERE product_tag_value = 'Bundle' AND skuable_type = 'Product'
    OR skuable_type = 'Bundle'



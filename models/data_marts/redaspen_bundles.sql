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
    SELECT p.product_id AS shopify_bundle_id,
        ip.product_id AS emma_bundle_id,
        (CASE
            WHEN p.product_title IS NULL THEN ip.product_title
            ELSE p.product_title
        END) AS bundle_title,
        ip.sku,
        ip.skuable_type
    FROM {{ ref("int_shopify__products") }} p JOIN bundle_tag bt ON p.product_id = bt.product_id
        RIGHT JOIN {{ ref("int_infotrax__products") }} ip ON p.emma_product_id = ip.product_id
    WHERE bt.value = 'Bundle'
        AND ip.skuable_type = 'Bundle'
)
SELECT *
FROM bundles
{{ config(database = 'redaspen') }}

{{ config(schema = 'bundles') }}

WITH infotrax_array AS(SELECT ol.bundle_properties[0]['bundle_order_line_id'] AS bundle_order_line_id,
    ol.order_id,
    ARRAY_AGG(sku) AS product_sku_array
FROM {{ ref("redaspen_order_lines") }} ol
WHERE bundle_properties IS NOT NULL
    AND source = 'Infotrax'
GROUP BY bundle_order_line_id, ol.order_id
),
loyalty_box_array AS(SELECT ARRAY_AGG(ol.sku) AS product_sku_array,
                        ol.order_id,
                        ol.bundle_properties 
            FROM {{ ref('redaspen_order_lines') }} ol
            WHERE ol.bundle_properties[0]['loyalty_box_order_id'] IS NOT NULL
            GROUP BY ol.bundle_properties, ol.order_id
),
bundle_union AS(
    --Infotrax Bundles with Components
    SELECT DISTINCT(ol.bundle_properties[0]['bundle_order_line_id']) AS bundle_order_line_id,
        ol.bundle_properties[0]['infotrax_order_number'] AS order_id,
        ol.bundle_properties[0]['price'] AS price_cents,
        ol.bundle_properties[0]['quantity'] AS quantity_ordered,
        ol.bundle_properties[0]['set']::varchar AS bundle_sku,
        ol.bundle_properties[0]['set_name']::varchar AS bundle_name,
        ol.bundle_properties[0]['total_amount'] AS pre_tax_price_cents,
        ia.product_sku_array AS product_sku_array,
        ol.source
    FROM {{ ref("redaspen_order_lines") }} ol LEFT JOIN infotrax_array ia ON ol.bundle_properties[0]['bundle_order_line_id'] = ia.bundle_order_line_id
    WHERE source = 'Infotrax'
UNION
    --Infotrax Bundles with no Components
    SELECT order_line_id,
        order_id,
        price_cents,
        quantity_ordered,
        sku AS bundle_sku,
        product_name AS bundle_name,
        pre_tax_price_cents,
        NULL AS product_sku_array,
        source
    FROM {{ ref("redaspen_order_lines") }}
    WHERE skuable_type = 'Bundle'
        AND bundle_properties IS NULL
        OR ARRAY_SIZE(bundle_properties) = 0
UNION
--Shopify Bundles with Components
SELECT NULL AS order_line_id,
    order_id,
    price_cents,
    quantity_ordered,
    NULL AS bundle_sku,
    bundle_name,
    (price_cents * quantity_ordered) AS pre_tax_price_cents,
    product_sku_array,
    source
FROM (SELECT DISTINCT(ol.bundle_properties[2]['value']) AS distinction,
            ol.order_id,
            ol.bundle_properties[0]['value']::varchar AS bundle_name,
            CAST(REGEXP_REPLACE(ol.bundle_properties[1]['value'], '\\$', '')*100 AS number) AS price_cents,
            SPLIT(ol.bundle_properties[4]['value'], ',') AS product_sku_array,
            ol.bundle_properties[3]['value']::number AS quantity_ordered,
            source
    FROM {{ ref("redaspen_order_lines") }} ol 
    WHERE source = 'Shopify'
        AND ARRAY_SIZE(ol.bundle_properties) > 0
        AND ol.bundle_properties[0]['loyalty_box_order_id'] IS NULL
        AND TRY_CAST(REGEXP_REPLACE(ol.bundle_properties[1]['value'], '\\$', '') AS NUMBER) IS NOT NULL) 
UNION
--Shopify Loyalty Box
SELECT ol.bundle_properties[0]['loyalty_box_order_line_id'] AS order_line_id,
    ol.bundle_properties[0]['loyalty_box_order_id'] AS order_id,
    ol.bundle_properties[0]['loyalty_box_price']*100 AS price_cents,
    ol.bundle_properties[0]['loyalty_box_order_quantity'] AS quantity_ordered,
    ol.bundle_properties[0]['loyalty_box_sku'] AS bundle_sku,
    ol.bundle_properties[0]['loyalty_box_title'] AS bundle_name,
    ol.bundle_properties[0]['loyalty_box_total']*100 AS pre_tax_price_cents,
    lba.product_sku_array,
    ol.source
FROM {{ ref("redaspen_order_lines") }} ol RIGHT JOIN loyalty_box_array lba ON ol.order_id = lba.order_id
WHERE ol.bundle_properties[0]['loyalty_box_order_id'] IS NOT NULL
)
SELECT bu.bundle_order_line_id,
    bu.order_id,
    CAST(bu.price_cents AS number) AS price_cents,
    bu.quantity_ordered,
    bu.bundle_sku,
    bu.bundle_name,
    CAST(bu.pre_tax_price_cents AS number) AS pre_tax_price_cents,
    array_sort(bu.product_sku_array) AS product_sku_array,
    bu.source,
    o.distributor_status,
    o.created_at
FROM bundle_union bu /*LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON bu.bundle_sku = bv.sku
    OR bu.bundle_name = bv.shopify_bundle_title */ --We need a better way to categorize loyalty boxes. We have overlapping skus, which is the only item that Infotrax order lines connects on for product id's.
LEFT JOIN {{ ref("redaspen_orders") }} o ON bu.order_id = o.order_id
--Cast as number once fixes are made


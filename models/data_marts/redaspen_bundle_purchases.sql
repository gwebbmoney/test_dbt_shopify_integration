{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'bundles')}}

WITH infotrax_array AS(SELECT ol.bundle_properties[0]['bundle_order_line_id'] AS bundle_order_line_id,
    ol.order_id,
    ARRAY_AGG(sku) AS product_sku_array
FROM {{ ref("redaspen_order_lines") }} ol
WHERE bundle_properties IS NOT NULL
    AND source = 'Infotrax'
GROUP BY bundle_order_line_id, ol.order_id
),
bundle_union AS(
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
UNION
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
            CAST(REGEXP_REPLACE(ol.bundle_properties[1]['value'], '\\$', '')*100 AS NUMBER) AS price_cents,
            SPLIT(ol.bundle_properties[4]['value'], ',') AS product_sku_array,
            ol.bundle_properties[3]['value']::number AS quantity_ordered,
            source
    FROM {{ ref("redaspen_order_lines") }} ol
    WHERE source = 'Shopify'
        AND ARRAY_SIZE(ol.bundle_properties) > 0)
)
SELECT bu.bundle_order_line_id,
    bu.order_id,
    CAST(bu.price_cents AS NUMBER) AS price_cents,
    bu.quantity_ordered,
    bu.bundle_sku,
    bu.bundle_name,
    CAST(bu.pre_tax_price_cents AS NUMBER) AS pre_tax_price_cents,
    array_sort(bu.product_sku_array) AS product_sku_array,
    bu.source,
    bv.bundle_type,
    o.distributor_status,
    o.created_at
FROM bundle_union bu LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON bu.bundle_sku = bv.sku
    OR bu.bundle_name = bv.shopify_bundle_title 
LEFT JOIN {{ ref("redaspen_orders") }} o ON bu.order_id = o.order_id





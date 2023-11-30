WITH infotrax_array AS(SELECT ol.bundle_properties[0]['bundle_order_line_id'] AS order_line_id,
    ol.order_id,
    ARRAY_AGG(sku) AS product_sku_array
FROM {{ ref("redaspen_order_lines") }} ol
WHERE bundle_properties IS NOT NULL
    AND source = 'Infotrax'
GROUP BY order_line_id, ol.order_id
ORDER BY order_line_id
),
bundle_union AS(
    SELECT DISTINCT(fol.value:bundle_order_line_id::number) AS order_line_id,
        fol.value:infotrax_order_number::number AS order_id,
        fol.value:price::number AS price_cents,
        fol.value:quantity::number AS quantity_ordered,
        fol.value:set::varchar AS sku,
        fol.value:set_name::varchar AS bundle_name,
        fol.value:total_amount::number AS pre_tax_price_cents,
        ia.product_sku_array,
        ol.source
    FROM {{ ref("redaspen_order_lines") }} ol,
        LATERAL FLATTEN (input => ol.bundle_properties) fol
    LEFT JOIN infotrax_array ia ON fol.order_line_id = ia.order_line_id
    WHERE source = 'Infotrax'
UNION
    SELECT order_line_id,
        order_id,
        price_cents,
        quantity_ordered,
        sku,
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
    NULL AS sku,
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
SELECT bu.*,
    bv.bundle_type
FROM bundle_union bu LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON bu.sku = bv.sku
    OR bu.bundle_name = bv.shopify_bundle_title 





WITH bundle_union AS(
    SELECT DISTINCT(fol.value:bundle_order_line_id::number) AS order_line_id,
        fol.value:infotrax_order_number::number AS order_id,
        fol.value:price::number AS price_cents,
        fol.value:quantity::number AS quantity_ordered,
        fol.value:set::varchar AS sku,
        fol.value:set_name::varchar AS bundle_name,
        fol.value:total_amount::number AS pre_tax_price_cents,
        ol.source
    FROM {{ ref("redaspen_order_lines") }} ol,
        LATERAL FLATTEN (input => ol.bundle_properties) fol
    WHERE source = 'Infotrax'
UNION
    SELECT order_line_id,
        order_id,
        price_cents,
        quantity_ordered,
        sku,
        product_name AS bundle_name,
        pre_tax_price_cents,
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
    price_cents,
    source
FROM (SELECT DISTINCT(ol.bundle_properties[2]['value']) AS distinction,
            ol.order_id,
            ol.bundle_properties[0]['value']::varchar AS bundle_name,
            CAST(REGEXP_REPLACE(ol.bundle_properties[1]['value'], '\\$', '')*100 AS NUMBER) AS price_cents,
            ol.bundle_properties[3]['value']::number AS quantity_ordered,
            source
    FROM {{ ref("redaspen_order_lines") }} ol
    WHERE source = 'Shopify'
        AND ARRAY_SIZE(ol.bundle_properties) > 0)
)
SELECT *
FROM bundle_union bu LEFT JOIN {{ ref("redaspen_bundle_variants") }} bv ON bu.sku = bv.sku
    OR bu.bundle_name = bv.shopify_bundle_title




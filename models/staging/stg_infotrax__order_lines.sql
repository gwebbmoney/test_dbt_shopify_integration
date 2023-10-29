WITH raw_order_lines AS(
    SELECT * FROM {{ source('raw_infotrax', 'ORDERLINES') }}
),
raw_infotrax_orders AS(
    SELECT * FROM {{ ref("stg_infotrax__orders")}}
),
product_bundle_base AS(
    SELECT s.name AS sku,
        p.name as product_name, 
        p.id as product_id,
        s.id as sku_id
    FROM {{ source("redaspen", 'PRODUCTS') }} p
        LEFT JOIN {{ source("redaspen", 'SKUS') }} s ON p.id = s.skuable_id
    WHERE s.skuable_type = 'Product'
    UNION
    SELECT s.name as sku,
        b.name,
        b.id,
        s.id
    FROM {{ source("redaspen", 'BUNDLES') }} b
        LEFT JOIN {{ source("redaspen", 'SKUS') }} s ON b.id = s.skuable_id
    WHERE s.skuable_type = 'Bundle'
)
SELECT ol.id,
    ol.sales_price * 100 as sales_price_cents,
    pbb.product_id,
    ol.order_number as infotrax_order_number,
    COALESCE(pbb.product_name, ol.item_name_1) AS product_name,
    ol.quantity_ordered,
    ol.quantity_returned,
    ol.order_line,
    ol.price_1 * 100 as retail_amount_cents,
    ol.price_2 * 100 as pv_qualifying_amount_cents,
    ol.price_3 * 100 as taxable_amount_cents,
    ol.price_4 * 100 as commissionable_volume_cents,
    ol.quantity_shipped,
    ol.sales_category,
    ol.taxable_price * 100 as taxable_price_cents,
    ol.last_update_time as updated_at,
    ol.item_code as infotrax_sku,
    ol.flag_2 as component_status,
    rio.currency_code,
    pbb.sku_id,
    ol.promo_id,
    ol.kit_line,
    ol._FIVETRAN_DELETED
FROM raw_order_lines ol LEFT JOIN raw_infotrax_orders rio ON ol.order_number = rio.infotrax_order_number
    LEFT JOIN product_bundle_base pbb ON pbb.sku = ol.item_code
WHERE ol._FIVETRAN_DELETED = FALSE



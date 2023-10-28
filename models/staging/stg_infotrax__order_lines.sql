WITH raw_order_lines AS(
    SELECT * FROM {{ source('raw_infotrax', 'ORDERLINES') }}
),
raw_infotrax_orders AS(
    SELECT * FROM {{ ref("stg_infotrax__orders")}}
)
SELECT ol.id,
    ol.sales_price * 100 as sales_price_cents,
    ol.order_number as infotrax_order_number,
    ol.item_name_1 as product_name,
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
    o.currency_code,
    ol.promo_id,
    ol.kit_line,
    ol._FIVETRAN_DELETED
FROM raw_order_lines orl LEFT JOIN raw_infotrax_orders rio ON orl.order_number = rio.infotrax_order_number
WHERE ol._FIVETRAN_DELETED = FALSE



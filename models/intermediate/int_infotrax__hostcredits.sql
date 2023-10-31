WITH processed_orders AS(
    SELECT * FROM {{ ref("redaspen_processed_orders") }}
),
order_lines AS(
    SELECT * FROM {{ ref("stg_infotrax__order_lines") }}
    WHERE infotrax_sku = 'HOSTCREDIT'
)
SELECT ol.infotrax_order_number AS order_id,
    (ol.retail_amount_cents * -1) AS hostcredit_amount_cents,
    po.created_at,
    po.brand_ambassador_id,
    po.distributor_status
FROM order_lines ol JOIN processed_orders po ON ol.infotrax_order_number = po.infotrax_order_number
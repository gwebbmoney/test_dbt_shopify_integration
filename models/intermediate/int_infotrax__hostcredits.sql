-- Creates view that houses infotrax hostcredits
WITH processed_orders AS(
    SELECT * FROM {{ ref("shopify_processed_orders") }}
-- Grabs shopify processed orders
),
order_lines AS(
    SELECT * FROM {{ ref("stg_infotrax__order_lines") }}
    WHERE infotrax_sku = 'HOSTCREDIT'
-- Grabs infotrax order lines
)
SELECT po.order_id,
    (ol.retail_amount_cents * -1) AS hostcredit_amount_cents,
    po.created_at,
    po.brand_ambassador_id,
    po.distributor_status
FROM order_lines ol JOIN processed_orders po ON ol.infotrax_order_number = po.order_id
-- Organizes view into it's final format
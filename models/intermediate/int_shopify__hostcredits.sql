-- Creates view for Shopify hostcredits
-- NOTE: Hostcredits are pop up rewards. Infotrax classified pop up rewards as hostcredits
WITH hostcredits AS(
    SELECT *
    FROM {{ ref("shopify_discount_and_promotion_orders") }}
    WHERE ORDER_DISCOUNT_CODE LIKE '%POP%'
-- Finds all discounts/promotions where the discount code contains 'POP'
),
processed_orders AS(
    SELECT *
    FROM  {{ ref("shopify_processed_orders") }}
-- Grabs shopify processed orders
-- Used to reference what order used a pop up code
)
SELECT po.order_id,
    h.total_discount_amount_cents,
    h.created_at,
    po.brand_ambassador_id,
    po.distributor_status
FROM hostcredits h JOIN processed_orders po ON h.order_id = po.order_id
-- Organizes hostcredits query into it's final format
WITH hostcredits AS(
    SELECT *
    FROM {{ ref("shopify_discount_and_promotion_orders") }}
    WHERE ORDER_DISCOUNT_CODE LIKE '%POP%'
),
processed_orders AS(
    SELECT *
    FROM  {{ ref("shopify_processed_orders") }}
)
SELECT po.order_id,
    h.total_discount_amount_cents,
    h.created_at,
    po.brand_ambassador_id,
    po.distributor_status
FROM hostcredits h JOIN processed_orders po ON h.order_id = po.order_id
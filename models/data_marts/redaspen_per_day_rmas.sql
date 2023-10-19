WITH orders AS(
    SELECT * FROM {{ ref('redaspen_processed_orders') }}
)
SELECT created_at::date AS day,
    SUM(order_refund_amount_cents) AS total_rmas_cents
FROM orders
GROUP BY day 


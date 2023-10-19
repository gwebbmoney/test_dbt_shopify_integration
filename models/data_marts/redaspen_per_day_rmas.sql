WITH orders AS(
    SELECT * FROM {{ ref("int_redaspen__refunds") }}
)
SELECT refunded_at::date AS day,
    SUM(refund_amount_cents) AS total_rmas_cents
FROM orders
GROUP BY day 


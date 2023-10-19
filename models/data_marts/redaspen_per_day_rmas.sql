WITH orders AS(
    SELECT * FROM {{ ref('redaspen_processed_orders') }}
)
SELECT created_at::date AS day,
    SUM(refund_invoice_amount) AS total_rmas_cents
FROM orders
GROUP BY day 


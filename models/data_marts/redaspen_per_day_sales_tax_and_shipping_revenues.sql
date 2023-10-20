WITH per_day_sales_tax_and_shipping_revenue AS(
    SELECT created_at::date AS day,
        SUM(sales_tax_amount_cents - sales_tax_refund_cents) AS sales_tax_cents,
        SUM(shipping_amount_cents - shipping_refund_cents) AS shipping_amount_cents
    FROM {{ ref("redaspen_processed_orders") }}
    GROUP BY day
)
SELECT *
FROM per_day_sales_tax_and_shipping_revenue
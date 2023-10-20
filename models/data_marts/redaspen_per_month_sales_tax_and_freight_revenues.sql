WITH per_month_sales_tax_and_freight_revenue AS(
    SELECT MONTH(day) AS month,
        YEAR(day) AS year,
        SUM(sales_tax_cents) AS sales_tax_cents,
        SUM(shipping_amount_cents) AS shipping_amount_cents
    FROM {{ ref("redaspen_per_day_sales_tax_and_shipping_revenues") }}
    GROUP BY month, year
)
SELECT *
FROM per_month_sales_tax_and_freight_revenue
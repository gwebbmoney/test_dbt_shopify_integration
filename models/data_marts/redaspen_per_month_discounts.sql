SELECT MONTH(day) AS month,
    YEAR(day) AS YEAR,
    distributor_status,
    COALESCE(SUM(total_discount_amount_cents), 0) AS total_discount_amount_cents
FROM {{ ref("redaspen_per_day_discounts") }}
GROUP BY month, year, distributor_status
ORDER BY month, year
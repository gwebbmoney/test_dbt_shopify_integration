SELECT YEAR(day) AS year,
    MONTH(day) AS month,
    SUM(total_rmas_cents) AS total_rmas_cents
FROM {{ ref("redaspen_per_day_rmas") }}
GROUP BY year,
    month
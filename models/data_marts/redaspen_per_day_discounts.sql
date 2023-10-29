WITH days AS({{dbt_utils.date_spine(
    datepart = "day",
    start_date = "cast('2020-01-01' as date)",
    end_date = 'current_date'
)}})
SELECT date_part AS day,
    dpo.distributor_status,
    COALESCE(SUM(dpo.total_discount_amount_cents), 0) AS total_discount_amount_cents
FROM days d LEFT JOIN {{ ref("redaspen_discount_and_promotion_orders") }} dpo ON d.date_part = dpo.created_at::date
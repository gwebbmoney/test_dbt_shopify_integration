WITH order_refund AS(
    SELECT * FROM {{ ref("stg_infotrax_order_refund") }}
)
SELECT infotrax_order_number AS order_id,
    refunded_at,
    processed_at,
    refund_bonus_period AS bonus_period,
    refund_invoice_amount AS refund_amount_cents
FROM order_refund
WHERE refund_order_status NOT IN (1,9)





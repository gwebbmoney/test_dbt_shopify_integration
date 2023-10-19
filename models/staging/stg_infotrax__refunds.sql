WITH order_refund AS(
    SELECT * FROM {{ ref("stg_infotrax_order_refund") }}
)
SELECT infotrax_order_number,
    refunded_at,
    processed_at,
    bonus_period,
    refund_invoice_amount AS refund_amount_cents
FROM order_refund
-- Creates a staging file for infotrax refunds
-- Used to combine with all Shopify refunds
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
-- Order status 1,9 are deleted orders
-- We do not included Infotrax deleted orders with refunds





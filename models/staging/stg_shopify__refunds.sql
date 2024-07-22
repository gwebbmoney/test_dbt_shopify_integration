-- Tracks shopify refunds and links these refunds to a official transaction
WITH refunds AS(SELECT id,
                created_at::timestamp_ntz,
                processed_at::timestamp_ntz,
                note,
                restock,
                order_id
            FROM {{ source('shopify_raw', 'REFUND') }}
),
transactions AS(SELECT id,
                order_id,
                refund_id,
                amount*100 AS refund_amount_cents,
                created_at::timestamp_ntz AS refunded_at,
                processed_at::timestamp_ntz AS processed_at
            FROM {{ source('shopify_raw', 'TRANSACTION') }}
            WHERE kind = 'refund'
                AND status = 'success'
                --AND test = FALSE
)
SELECT t.order_id, 
    r.id AS refund_id,
    t.refunded_at,
    t.processed_at,
    DATE_TRUNC('month', t.refunded_at::date) AS bonus_period,
    t.refund_amount_cents,
    r.note,
    r.restock
FROM transactions t LEFT JOIN refunds r ON t.order_id = r.order_id
    AND t.refund_id = r.id


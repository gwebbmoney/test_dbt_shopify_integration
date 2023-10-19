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
                created_at::timestamp_ntz,
                processed_at::timestamp_ntz
            FROM {{ source('shopify_raw', 'TRANSACTION') }}
            WHERE kind = 'refund'
                AND status = 'success'
                --AND test = FALSE
)
SELECT t.order_id, 
    r.id AS refund_id,
    t.created_at AS refunded_at,
    t.processed_at,
    DATE_TRUNC('month', t.created_at::date) AS bonus_period,
    t.refund_amount_cents,
    r.note,
    r.restock
FROM transactions t LEFT JOIN refunds r ON t.order_id = r.order_id


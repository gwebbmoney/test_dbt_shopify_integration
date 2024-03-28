WITH transactions AS(
    SELECT * FROM {{ source('shopify_raw', 'TRANSACTION') }}
    WHERE GATEWAY = 'gift_card'
        AND kind = 'sale'
),
processed_orders AS(
    SELECT * FROM {{ ref('shopify_processed_orders') }}
)
SELECT t.order_id,
    t.amount AS hostcredit_amount_cents,
    t.created_at,
    po.brand_ambassador_id
FROM transactions t JOIN processed_orders po ON t.order_id = po.order_id
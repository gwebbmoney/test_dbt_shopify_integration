WITH discount_code AS(SELECT order_id,    
                        index,
                        code,
                        type,
                        amount
                    FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }}
),
orders AS(SELECT order_id,
            total_discount_amount_cents,
            created_at
        FROM {{ ref("redaspen_processed_orders") }}
)
SELECT o.order_id,
    dc.index,
    dc.code AS order_discount_code,
    dc.type,
    o.total_discount_amount_cents,
    o.created_at
FROM discount_code dc RIGHT JOIN orders o ON dc.order_id = o.order_id
WHERE total_discount_amount_cents > 0
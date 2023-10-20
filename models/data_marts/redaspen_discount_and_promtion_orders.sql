WITH discount_code AS(SELECT order_id,    
                        index,
                        code,
                        type,
                        amount
                    FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }}
),
orders AS(SELECT id,
            order_number,
            processed_at
        FROM {{ ref("redaspen_processed_orders") }}
)
SELECT dc.order_id,
    o.order_number,
    dc.index,
    dc.code AS order_discount_code,
    dc.type,
    dc.amount AS discount_amount,
    o.processed_at
FROM discount_code dc JOIN orders o ON dc.order_id = o.id
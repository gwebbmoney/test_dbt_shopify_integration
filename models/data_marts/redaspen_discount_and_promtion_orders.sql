WITH discount_code AS(SELECT order_id,    
                        index,
                        code AS order_discount_code,
                        type,
                        amount*100 AS total_discount_amount_cents
                    FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }}
),
infotrax_discounts AS(
    SELECT infotrax_order_number AS order_id,   
        product_name AS order_discount_name,
        retail_amount_cents AS total_discount_amount_cents
    FROM {{ ref("stg_infotrax__order_lines") }}
),
discount_union AS(
SELECT order_id,
    index,
    order_discount_code,
    NULL AS order_discount_name ,
    type,
    total_discount_amount_cent
FROM discount_code
UNION
SELECT order_id,
    NULL AS index,
    NULL AS order_discount_code,
    order_discount_name,
    NULL AS type,
    total_discount_amount_cents
FROM infotrax_discounts
),
orders AS(SELECT order_id,
            total_discount_amount_cents,
            created_at
        FROM {{ ref("redaspen_processed_orders") }}
)
SELECT du.*,
    o.created_at
FROM orders o LEFT JOIN discount_union du ON o.order_id = du.order_id
WHERE du.total_discount_amount_cents > 0
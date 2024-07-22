

{{ config(database = 'redaspen') }}

{{ config(schema = 'orders')}}

-- Creates a Transient Table within Snowflake that combines both Infotrax and Shopify discounts/promotions for an order
WITH discount_code AS(SELECT order_id,    
                        index,
                        code AS order_discount_code,
                        NULL AS order_discount_name,
                        type,
                        amount*100 AS total_discount_amount_cents
                    FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }}
-- Grabs all Shopify discount codes
),
infotrax_discounts AS(
    SELECT infotrax_order_number AS order_id,
        NULL AS index,
        NULL AS order_discount_code,
        NULL AS type,   
        product_name AS order_discount_name,
        (retail_amount_cents * -1) AS total_discount_amount_cents
    FROM {{ ref("stg_infotrax__order_lines") }}
    WHERE infotrax_sku = 'Discount'
-- Grabs all instances within Infotrax where there was discount
),
discount_union AS(
SELECT order_id,
    index,
    order_discount_code,
    order_discount_name,
    type,
    total_discount_amount_cents
FROM discount_code
UNION
SELECT order_id,
    index,
    order_discount_code,
    order_discount_name,
    type,
    total_discount_amount_cents
FROM infotrax_discounts
-- Combines both Infotrax and Shopify discounts
),
orders AS(SELECT order_id,
            total_discount_amount_cents,
            distributor_status,
            created_at
        FROM {{ ref("shopify_orders") }}
-- Grabs orders from both Shopify and Infotrax
)
SELECT du.*,
    o.distributor_status,
    o.created_at
FROM orders o LEFT JOIN discount_union du ON o.order_id = du.order_id
WHERE du.total_discount_amount_cents > 0 -- Include this so that only discounts are shown
-- Organizes table into it's final format
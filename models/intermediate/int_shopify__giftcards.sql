-- Creates view that houses Shopify gift cards
WITH transactions AS(
    SELECT * FROM {{ source('shopify_raw', 'TRANSACTION') }}
    WHERE GATEWAY = 'gift_card'
        AND kind = 'sale'
-- Finds gift cards that were used in a sale
),
processed_orders AS(
    SELECT * FROM {{ ref('shopify_processed_orders') }}
-- Grabs the shopify processed orders table
-- Used to link order id to each instance of a gift card use
)
SELECT t.order_id,
    t.amount AS hostcredit_amount_cents,
    t.created_at,
    po.brand_ambassador_id
FROM transactions t JOIN processed_orders po ON t.order_id = po.order_id
-- Organizes Shopify gift cards view into it's final format
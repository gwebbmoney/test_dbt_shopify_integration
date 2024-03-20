{{ config(database = 'redaspen') }}

{{ config(schema = 'orders')}}

WITH orders AS(
    SELECT * FROM {{ ref("shopify_orders") }}
)
SELECT *
FROM orders
WHERE (fulfillment_status NOT IN ('cancelled', 'unfulfilled') OR fulfillment_status IS NULL)
--Will probably have to change what qualifies for a processed order later
--For now, keep fulfillment_status as 'fulfilled'
--Questions: Is an item fulfilled if the order is completely refunded/partially refunded

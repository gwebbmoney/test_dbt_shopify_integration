{{ config(database = 'redaspen') }}

{{ config(schema = 'orders')}}

-- Creates a Transient Table within Snowflake that houses both Infotrax and Shopify processed orders
WITH orders AS(
    SELECT * FROM {{ ref("shopify_orders") }}
)
SELECT *
FROM orders
WHERE (fulfillment_status NOT IN ('cancelled', 'unfulfilled') OR fulfillment_status IS NULL)
-- A processed order is where the order was shipped or if fulfillment was not issued yet

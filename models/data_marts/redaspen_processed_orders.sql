

{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'orders')}}

-- NOTE: DO NOT USE. DATABASE NOT IN SNOWFLAKE ANYMORE
WITH orders AS(
    SELECT * FROM {{ ref("redaspen_orders") }}
)
SELECT *
FROM orders
WHERE financial_status = 'paid'
--Will probably have to change what qualifies for a processed order later
--For now, keep fulfillment_status as 'fulfilled'
--Questions: Is an item fulfilled if the order is completely refunded/partially refunded

WITH order_lines AS(
    SELECT * FROM {{ source("shopify_raw", 'ORDER_LINE') }}
)
SELECT *
FROM order_lines
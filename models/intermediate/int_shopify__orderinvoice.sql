WITH orders AS(
    SELECT * FROM {{ ref("stg_shopify__orders") }}
),
transactions AS(
    SELECT * FROM {{ ref("stg_shopify__transaction") }}
),
sale_transactions AS(SELECT DISTINCT(order_id),
                            SUM(amount) AS total_sale_transaction
                        FROM transactions
                        WHERE status = 'success'
                            AND kind = 'sale'
                        GROUP BY order_id
),
refund_transactions AS(SELECT DISTINCT(order_id),
                            SUM(amount) AS total_refund_transaction
                        FROM transactions
                        WHERE status = 'success'
                            AND kind = 'refund'
                        GROUP BY order_id
),
order_transaction AS(SELECT DISTINCT(o.id),
                        (CASE 
                            WHEN st.total_sale_transaction IS NULL THEN 0
                            ELSE st.total_sale_transaction
                        END) AS order_invoice,
                        (CASE
                            WHEN rt.total_refund_transaction IS NULL THEN 0
                            ELSE rt.total_refund_transaction
                        END) AS order_refund
                    FROM orders o LEFT JOIN sale_transactions st ON o.id = st.order_id
                    LEFT JOIN refund_transactions rt ON o.id = rt.order_id
),
order_invoice AS(SELECT DISTINCT(o.id),
                    o.order_number,
                    o.financial_status,
                    ot.order_invoice,
                    ot.order_refund
                FROM order_transaction ot RIGHT JOIN orders o ON ot.id = o.id
)
SELECT *
FROM order_invoice
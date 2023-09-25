WITH sale_transactions AS(SELECT order_id,
                            SUM(amount) AS total_sale_transaction
                        FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.TRANSACTION
                        WHERE status = 'success'
                            AND kind = 'sale'
                        GROUP BY order_id
),
refund_transactions AS(SELECT order_id,
                            SUM(amount) AS total_refund_transaction
                        FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.TRANSACTION
                        WHERE status = 'success'
                            AND kind = 'refund'
                        GROUP BY order_id
),
order_transaction AS(SELECT st.order_id,
                        (CASE
                            WHEN (st.total_sale_transaction - rt.total_refund_transaction) IS NULL THEN st.total_sale_transaction
                            ELSE (st.total_sale_transaction - rt.total_refund_transaction)
                        END) AS total_invoice
                    FROM sale_transactions st LEFT JOIN refund_transactions rt ON st.order_id = rt.order_id
),
order_invoice AS(SELECT o.id,
                    o.order_number,
                    o.financial_status,
                    (CASE
                        WHEN ot.total_invoice IS NULL THEN 0
                        ELSE ot.total_invoice
                    END) AS total_invoice_amount
                FROM order_transaction ot RIGHT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY."ORDER" o ON ot.order_id = o.id                
),
order_line_cond AS(SELECT   
                        order_id,
                        SUM(price * quantity - total_discount) AS line_item_subtotal
                    FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.ORDER_LINE
                    GROUP BY order_id
),
order_line_refund_cond AS(SELECT ol.order_id,
                        olr.order_line_id,
                        (CASE
                            WHEN SUM(olr.subtotal) IS NULL THEN 0
                            ELSE SUM(olr.subtotal)
                        END) AS line_item_refund,
                        (CASE
                            WHEN SUM(total_tax) IS NULL THEN 0
                            ELSE SUM(total_tax)
                        END) AS line_item_tax_refund
                    FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.ORDER_LINE_REFUND olr RIGHT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.ORDER_LINE ol ON olr.order_line_id = ol.id
                    GROUP BY ol.order_id,
                        olr.order_line_id
),
order_refund AS(SELECT order_id,
    SUM(line_item_refund) AS total_order_refund,
    SUM(line_item_tax_refund) AS total_tax_refund
FROM order_line_refund_cond olrc
GROUP BY order_id
),
order_line_subtotal AS(SELECT olc.order_id,
                        (CASE
                            WHEN SUM(line_item_subtotal - total_order_refund) IS NULL THEN 0
                            ELSE SUM(line_item_subtotal - total_order_refund)
                        END) AS total_subtotal_amount
                    FROM order_line_cond olc LEFT JOIN order_refund orf ON olc.order_id = orf.order_id
                    GROUP BY olc.order_id
),
order_line_tax_refund AS(SELECT order_id,
                            SUM(line_item_tax_refund) AS tax_refund
                        FROM order_line_refund_cond
                        GROUP BY order_id
),
order_discount AS(SELECT o.id,
                    (CASE
                        WHEN SUM(amount) IS NULL THEN 0
                        ELSE SUM(amount)
                    END) AS total_discount_amount
                    FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.ORDER_DISCOUNT_CODE odc RIGHT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY."ORDER" o ON odc.order_id = o.id
                    GROUP BY o.id
),
shipping_amount AS(SELECT o.id,
                        (CASE
                            WHEN SUM(osl.discounted_price) IS NULL THEN 0
                            ELSE SUM(osl.discounted_price)
                        END) AS total_shipping_amount
                    FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY."ORDER" o LEFT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.ORDER_SHIPPING_LINE osl ON o.id = osl.order_id
                    GROUP BY o.id
),
order_adjustment_cond AS(SELECT o.id,
                        (CASE
                            WHEN SUM(amount + tax_amount) IS NULL THEN 0
                            ELSE SUM(amount + tax_amount)
                        END)AS order_adjustment_amount
                    FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.ORDER_ADJUSTMENT oa RIGHT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY."ORDER" o ON oa.order_id = o.id
                    GROUP BY o.id
),
metadata AS(SELECT owner_id,
            key,
            value,
            owner_resource
        FROM FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.METAFIELD
        WHERE OWNER_RESOURCE = 'customer'
            AND key IN('InfoTraxID', 'MentorID', 'distributor_status')
),
distributor_info AS(SELECT DISTINCT(m.owner_id) AS customer_id,
    c.email,
    ca.name,
    MAX(CASE
        WHEN m.key = 'InfoTraxID' THEN m.value ELSE NULL END
    ) AS distributor_id,
    MAX(CASE
        WHEN m.key = 'MentorID' THEN m.value ELSE NULL END
    ) AS sponsor_id,
    MAX(CASE
        WHEN m.key = 'distributor_status' THEN m.value ELSE NULL END
    ) AS distributor_status
FROM metadata m LEFT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.CUSTOMER c ON c.id = m.owner_id 
    LEFT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY.CUSTOMER_ADDRESS ca ON c.id = ca.customer_id
GROUP BY m.owner_id, c.email, ca.name
ORDER BY m.owner_id
),
option_two AS(SELECT oi.id AS order_id,
    oi.order_number,
    ols.total_subtotal_amount*100 AS total_subtotal_amount_cents,
    od.total_discount_amount*100 AS total_discount_amount_cents,
    sa.total_shipping_amount*100 AS total_shipping_amount_cents,
    oac.order_adjustment_amount*100 AS order_adjustment_amount_cents,
    o.current_total_tax*100 AS total_tax_amount_cents, 
    oi.total_invoice_amount*100 AS total_invoice_amount_cents,
    o.created_at,
    o.processed_at,
    o.cancelled_at,
    o.updated_at,
    o.cancel_reason,
    o.financial_status,
    o.fulfillment_status,
    o.shipping_address_address_1 AS shipping_address_one,
    o.shipping_address_address_2 AS shipping_address_two,
    o.shipping_address_city,
    o.shipping_address_province_code AS shipping_address_state,
    o.shipping_address_zip,
    o.shipping_address_longitude,
    o.shipping_address_latitude,
    o.billing_address_address_1 AS billing_address_one,
    o.billing_address_address_2 AS billing_address_two,
    o.billing_address_city,
    o.billing_address_province_code AS billing_address_state,
    o.billing_address_zip,
    o.billing_address_longitude,
    o.billing_address_latitude,
    o.customer_id,
    di.distributor_id,
    di.name,
    di.email,
    di.sponsor_id,
    di.distributor_status
FROM order_invoice oi JOIN order_line_subtotal ols ON oi.id = ols.order_id 
    LEFT JOIN order_discount od ON oi.id = od.id
    LEFT JOIN shipping_amount sa ON oi.id = sa.id
    LEFT JOIN order_adjustment_cond oac ON oi.id = oac.id
    LEFT JOIN FIVETRAN_SHOPIFY_RAW_DATA.SHOPIFY."ORDER" o ON oi.id = o.id
    LEFT JOIN distributor_info di ON o.customer_id = di.customer_id
)
SELECT ot.*
FROM option_two ot JOIN fivetran_shopify_raw_data.shopify."ORDER" o ON ot.order_id = o.id

-- Creates view that grabs and transforms Shopify order information
-- The reason for this being so complex was because there were several errors from Fivetran that did not match the actual order amount

WITH sale_transactions AS(SELECT DISTINCT(order_id),
                            SUM(amount) AS total_sale_transaction
                        FROM {{ source('shopify_raw', 'TRANSACTION') }}
                        WHERE status = 'success'
                            AND kind = 'sale'
                        GROUP BY order_id
-- Grabs all transaction information that were successful and resulted in a sale
),
refund_transactions AS(SELECT DISTINCT(order_id),
                            SUM(amount) AS total_refund_transaction
                        FROM {{ source('shopify_raw', 'TRANSACTION') }}
                        WHERE status = 'success'
                            AND kind = 'refund'
                        GROUP BY order_id
-- Grabs all transaction information that were successful and resulted in a refund
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
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN sale_transactions st ON o.id = st.order_id
                    LEFT JOIN refund_transactions rt ON o.id = rt.order_id
-- Combines sales transactions and refund transactions into one CTE
),
order_invoice AS(SELECT DISTINCT(o.id),
                    o.order_number,
                    o.financial_status,
                    ot.order_invoice,
                    ot.order_refund
                FROM order_transaction ot RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON ot.id = o.id
-- Creates the order invoice CTE that contains the order number of the order                
),
order_line_cond AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(price * quantity - total_discount) IS NULL THEN 0
                            ELSE SUM(price * quantity - total_discount)
                        END)AS line_item_subtotal
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_LINE') }} ol ON o.id = ol.order_id
                    GROUP BY o.id
-- Creates the subtotal amount of the order
-- Is calculated by summing the value of all the products for the order prior to taxes/order level discounts
),
order_line_refund_cond AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(olr.refund_price_cents) IS NULL THEN 0
                            ELSE SUM(olr.refund_price_cents)
                        END) AS subtotal_refund,
                        (CASE
                            WHEN SUM(olr.refund_tax_cents) IS NULL THEN 0
                            ELSE SUM(olr.refund_tax_cents)
                        END) AS total_tax_refund
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_LINE') }} ol ON o.id = ol.order_id
                    LEFT JOIN {{ ref('int_shopify__order_line_refund') }} olr ON ol.id = olr.order_line_id
                    GROUP BY o.id
-- Calculates subtotal and tax refunds
),
order_discount AS(SELECT DISTINCT(o.id),
                    (CASE
                        WHEN SUM(amount) IS NULL THEN 0
                        ELSE SUM(amount)
                    END) AS total_discount_amount
                    FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }} odc RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON odc.order_id = o.id
                    GROUP BY o.id
-- Calculates the total discount for an order
),
shipping_amount AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(osl.discounted_price) IS NULL THEN 0
                            ELSE SUM(osl.discounted_price)
                        END) AS total_shipping_amount
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_SHIPPING_LINE') }} osl ON o.id = osl.order_id
                    GROUP BY o.id
-- Calculates the shipping amount for an order
),
shipping_tax_amount AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(ostl.price) IS NULL THEN 0
                            ELSE SUM(ostl.price)
                        END) AS shipping_tax_amount
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_SHIPPING_LINE') }} osl ON o.id = osl.order_id
                        LEFT JOIN {{ source('shopify_raw', 'ORDER_SHIPPING_TAX_LINE') }} ostl ON osl.id = ostl.order_shipping_line_id
                    GROUP BY o.id
-- Calculates the shipping tax amount for an order
),
tax_lines_cond AS(SELECT DISTINCT(o.id),
                    (CASE
                        WHEN SUM(tl.price) IS NULL THEN 0
                        ELSE SUM(tl.price)
                    END) AS total_tax_amount
                FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_LINE') }} ol ON o.id = ol.order_id
                    LEFT JOIN {{ source('shopify_raw', 'TAX_LINE') }} tl ON ol.id = tl.order_line_id
                GROUP BY o.id
-- Calculates total tax amount for an order
),
order_adjustment_shipping AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(amount) IS NULL THEN 0
                            WHEN kind = 'shipping_refund' THEN (SUM(amount)*-1)
                        END) AS shipping_refund,
                        (CASE
                            WHEN SUM(tax_amount) IS NULL THEN 0
                            WHEN kind = 'shipping_refund' THEN SUM(tax_amount) 
                        END) AS shipping_tax_refund
                        FROM {{ source('shopify_raw', 'ORDER_ADJUSTMENT') }} oa RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON oa.order_id = o.id
                        WHERE kind = 'shipping_refund'
                        GROUP BY o.id, oa.kind
-- Calculates order adjustments in reference to shipping for an order
-- Main order adjustments are shipping refunds and shipping tax refunds
),
order_adjustment_cond AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(amount) IS NULL THEN 0
                            WHEN kind <> 'shipping_refund' THEN SUM(amount)
                        END) AS order_adjustment_amount,
                        (CASE
                            WHEN SUM(tax_amount) IS NULL THEN 0
                            ELSE SUM(tax_amount)
                        END) AS order_adjustment_tax_amount
                    FROM {{ source('shopify_raw', 'ORDER_ADJUSTMENT') }} oa RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON oa.order_id = o.id
                    WHERE kind <> 'shipping_refund'
                    GROUP BY o.id, oa.kind
-- Calculates order adjustments for an order
),
order_tag AS(
    SELECT DISTINCT(order_id) AS order_id,
    (CASE
        WHEN value = 'Subscription First Order' THEN 'Subscription_First_Order'
        WHEN value = 'Subscription Recurring Order' THEN 'Subscription_Recurring_Order'
        WHEN value = 'Enrollment Order' THEN 'Enrollment_Order'
    END) AS order_tag_type
FROM {{ source("shopify_raw", 'ORDER_TAG') }}
WHERE value IN ('Subscription First Order', 'Subscription Recurring Order', 'Enrollment Order')
-- Grabs necessary order tags for an order
-- Only include order tags that pertain to subscriptions and enrollment orders, such as business kit purchases
),
order_tag_cond AS(
    SELECT order_id,
        ARRAY_AGG(order_tag_type) AS order_tag_type
    FROM order_tag
    GROUP BY order_id
-- Combines all order tags and puts them into an array
-- We do this because multiple tags can be put into one order
),
redeemed_pop_up AS(
    SELECT DISTINCT(order_id) AS order_id,
    (CASE
        WHEN value = 'Pop-Up Code' THEN TRUE 
        ELSE FALSE
    END) AS redeemed_pop_up_reward
FROM {{ source("shopify_raw", 'ORDER_TAG') }}
WHERE value ='Pop-Up Code'
-- Creates boolean to see if a pop up code was redeemed on an order
),
distributor_status_metafield AS(
    SELECT DISTINCT(owner_id) AS order_id,
    (CASE
        WHEN value = 'Consumer Order' THEN 'C'
        WHEN value = 'Affiliate Order' THEN 'A'
        WHEN value = 'Distributor Order' THEN 'D'
    END) AS distributor_status
FROM {{ source('shopify_raw', 'METAFIELD') }}
WHERE value IN ('Consumer Order', 'Distributor Order', 'Affiliate Order')
-- Attaches distributor status to the order
-- This is the historical distributor status. Essentially, this is the status of the customer/distributor at their time or purchase
),
pv_qual_field AS(
    SELECT DISTINCT(owner_id) AS order_id,
        ROUND(value, 2)*100 AS pv_qualifying_amount
    FROM {{ source('shopify_raw', 'METAFIELD') }} 
    WHERE key = 'pv_qualifying_amount'
-- Attaches PV Qualifying Amount field to the order
),
customers AS(
    SELECT shopify_customer_id,
        brand_ambassador_id
    FROM {{ ref('shopify_distributors') }}
-- Creates CTE that contains the Shopify Customer ID and their Infotrax BA ID
)
SELECT DISTINCT(oi.id) AS order_id,
    oi.order_number,
    COALESCE(olc.line_item_subtotal*100, 0) AS subtotal_amount_cents,
    COALESCE(tlc.total_tax_amount*100, 0) AS sales_tax_amount_cents,
    COALESCE(sa.total_shipping_amount*100, 0) AS shipping_amount_cents,
    COALESCE(sta.shipping_tax_amount*100, 0) AS shipping_tax_amount_cents,
    COALESCE(od.total_discount_amount*100, 0) AS total_discount_amount_cents,
    COALESCE(oi.order_invoice*100, 0) AS order_invoice_amount_cents,
    CONVERT_TIMEZONE('America/Boise', o.created_at) AS created_at,
    CONVERT_TIMEZONE('America/Boise', o.processed_at) AS processed_at,
    CONVERT_TIMEZONE('America/Boise', o.cancelled_at) AS cancelled_at,
    CONVERT_TIMEZONE('America/Boise', o.updated_at) AS updated_at,
    DATE(DATE_TRUNC('month', created_at)) AS bonus_period,
    o.cancel_reason,
    o.financial_status,
    o.fulfillment_status,
    ABS(orf.subtotal_refund) AS subtotal_refund_cents,
    ABS(orf.total_tax_refund) AS sales_tax_refund_cents,
    COALESCE(ABS(oas.shipping_refund)*100, 0) AS shipping_refund_cents,
    COALESCE(ABS(oas.shipping_tax_refund)*100, 0) AS shipping_tax_refund_cents,
    COALESCE(oac.order_adjustment_amount*100, 0) AS order_adjustment_amount_cents,
    COALESCE(oac.order_adjustment_tax_amount*100, 0) AS order_adjustment_tax_amount_cents,
    COALESCE(oi.order_refund*100, 0) AS order_refund_amount_cents,
    (order_invoice_amount_cents - order_refund_amount_cents)::number AS total_order_amount_cents,
    COALESCE(pvq.pv_qualifying_amount, 0)::number AS pv_qualifying_amount_cents,
    o.shipping_address_first_name,
    o.shipping_address_last_name,
    o.shipping_address_name,
    o.shipping_address_address_1 AS shipping_address_one,
    o.shipping_address_address_2 AS shipping_address_two,
    o.shipping_address_city,
    o.shipping_address_province_code AS shipping_address_state,
    o.shipping_address_zip,
    o.shipping_address_longitude,
    o.shipping_address_latitude,
    o.billing_address_first_name,
    o.billing_address_last_name,
    o.billing_address_name,
    o.billing_address_address_1 AS billing_address_one,
    o.billing_address_address_2 AS billing_address_two,
    o.billing_address_city,
    o.billing_address_province_code AS billing_address_state,
    o.billing_address_zip,
    o.billing_address_longitude,
    o.billing_address_latitude,
    o.customer_id,
    c.brand_ambassador_id,
    o.user_id,
    o.checkout_id,
    dsm.distributor_status,
    o.token,
    o.cart_token,
    o.checkout_token,
    o.referring_site,
    o.app_id,
    o.buyer_accepts_marketing,
    o.browser_ip,
    o.landing_site_base_url,
    COALESCE(otc.order_tag_type, []) AS order_tag_type,
    (CASE WHEN rpu.redeemed_pop_up_reward IS NULL THEN FALSE ELSE rpu.redeemed_pop_up_reward END) AS redeemed_pop_up_reward,
    COALESCE(REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"SponsorID"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS sponsor_id,
    COALESCE(REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"PartyID"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS partyid,
    COALESCE(REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"HostID"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS hostid,
    COALESCE(REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"dateOfBirth"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS dateofbirth,
    COALESCE(REGEXP_SUBSTR(note_attributes, '"name"\s*:\s*"referrer"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS referrer,
    COALESCE(REGEXP_SUBSTR(note_attributes, '"name"\s*:\s*"pws"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS pws,
    COALESCE(REGEXP_SUBSTR(note_attributes, '"name"\s*:\s*"PartyWebAlias"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1), 'NONE') AS partywebalias,
    o.note_attributes,
    o.client_details_user_agent,
    SPLIT_PART(SPLIT_PART(o.note, '\n', 2), ':', 2) AS sphere_order_number_reference,
    SPLIT_PART(SPLIT_PART(o.note, '\n', 3), ':', 2) AS infotrax_order_number_reference,
    o.test,
    o._fivetran_deleted,
    o._fivetran_synced
FROM order_invoice oi JOIN order_line_cond olc ON oi.id = olc.id 
    LEFT JOIN order_line_refund_cond orf ON oi.id = orf.id
    LEFT JOIN tax_lines_cond tlc ON oi.id = tlc.id
    LEFT JOIN order_discount od ON oi.id = od.id
    LEFT JOIN shipping_amount sa ON oi.id = sa.id
    LEFT JOIN shipping_tax_amount sta ON oi.id = sta.id
    LEFT JOIN order_adjustment_shipping oas ON oi.id = oas.id
    LEFT JOIN order_adjustment_cond oac ON oi.id = oac.id
    LEFT JOIN order_tag_cond otc ON oi.id = otc.order_id
    LEFT JOIN redeemed_pop_up rpu ON oi.id = rpu.order_id
    LEFT JOIN distributor_status_metafield dsm ON oi.id = dsm.order_id
    LEFT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON oi.id = o.id
    LEFT JOIN customers c ON o.customer_id = c.shopify_customer_id
    LEFT JOIN pv_qual_field pvq ON oi.id = pvq.order_id
WHERE o._fivetran_deleted = FALSE
-- Organizes order table into its final format

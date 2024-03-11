WITH sale_transactions AS(SELECT DISTINCT(order_id),
                            SUM(amount) AS total_sale_transaction
                        FROM {{ source('shopify_raw', 'TRANSACTION') }}
                        WHERE status = 'success'
                            AND kind = 'sale'
                        GROUP BY order_id
),
refund_transactions AS(SELECT DISTINCT(order_id),
                            SUM(amount) AS total_refund_transaction
                        FROM {{ source('shopify_raw', 'TRANSACTION') }}
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
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN sale_transactions st ON o.id = st.order_id
                    LEFT JOIN refund_transactions rt ON o.id = rt.order_id
),
order_invoice AS(SELECT DISTINCT(o.id),
                    o.order_number,
                    o.financial_status,
                    ot.order_invoice,
                    ot.order_refund
                FROM order_transaction ot RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON ot.id = o.id                
),
order_line_cond AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(price * quantity - total_discount) IS NULL THEN 0
                            ELSE SUM(price * quantity - total_discount)
                        END)AS line_item_subtotal
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_LINE') }} ol ON o.id = ol.order_id
                    GROUP BY o.id
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
),
order_discount AS(SELECT DISTINCT(o.id),
                    (CASE
                        WHEN SUM(amount) IS NULL THEN 0
                        ELSE SUM(amount)
                    END) AS total_discount_amount
                    FROM {{ source('shopify_raw', 'ORDER_DISCOUNT_CODE') }} odc RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON odc.order_id = o.id
                    GROUP BY o.id
),
shipping_amount AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(osl.discounted_price) IS NULL THEN 0
                            ELSE SUM(osl.discounted_price)
                        END) AS total_shipping_amount
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_SHIPPING_LINE') }} osl ON o.id = osl.order_id
                    GROUP BY o.id
),
shipping_tax_amount AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(ostl.price) IS NULL THEN 0
                            ELSE SUM(ostl.price)
                        END) AS shipping_tax_amount
                    FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_SHIPPING_LINE') }} osl ON o.id = osl.order_id
                        LEFT JOIN {{ source('shopify_raw', 'ORDER_SHIPPING_TAX_LINE') }} ostl ON osl.id = ostl.order_shipping_line_id
                    GROUP BY o.id
),
tax_lines_cond AS(SELECT DISTINCT(o.id),
                    (CASE
                        WHEN SUM(tl.price) IS NULL THEN 0
                        ELSE SUM(tl.price)
                    END) AS total_tax_amount
                FROM {{ source('shopify_raw', '"ORDER"') }} o LEFT JOIN {{ source('shopify_raw', 'ORDER_LINE') }} ol ON o.id = ol.order_id
                    LEFT JOIN {{ source('shopify_raw', 'TAX_LINE') }} tl ON ol.id = tl.order_line_id
                GROUP BY o.id
),
order_adjustment_cond AS(SELECT DISTINCT(o.id),
                        (CASE
                            WHEN SUM(amount) IS NULL THEN 0
                            WHEN kind = 'shipping_refund' THEN (SUM(amount)*-1)
                        END) AS shipping_refund,
                        (CASE
                            WHEN SUM(tax_amount) IS NULL THEN 0
                            WHEN kind = 'shipping_refund' THEN SUM(tax_amount) 
                        END) AS shipping_tax_refund,
                        (CASE
                            WHEN SUM(amount) IS NULL THEN 0
                            WHEN kind <> 'shipping_refund' THEN SUM(amount)
                        END) AS order_adjustment_amount,
                        (CASE
                            WHEN SUM(tax_amount) IS NULL THEN 0
                            ELSE SUM(tax_amount)
                        END) AS order_adjustment_tax_amount
                    FROM {{ source('shopify_raw', 'ORDER_ADJUSTMENT') }} oa RIGHT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON oa.order_id = o.id
                    GROUP BY o.id, oa.kind
),
order_tag_cond AS(
    SELECT DISTINCT(order_id) AS order_id,
    (CASE
        WHEN value = 'Subscription First Order' THEN 'Subscription_First_Order'
        WHEN value = 'Subscription Recurring Order' THEN 'Subscription_Recurring_Order'
        WHEN value = 'Enrollment Order' THEN 'Enrollment_Order'
        --WHEN value = 'Pop-Up Code' THEN 'Pop-up'
    END) AS order_tag_type,
    (CASE
        WHEN value = 'Pop-Up Code' THEN TRUE)
FROM {{ source("shopify_raw", 'ORDER_TAG') }}
WHERE value IN ('Subscription First Order', 'Subscription Recurring Order', 'Enrollment Order'/*, 'Pop-Up Code'*/)
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
),
customers AS(
    SELECT shopify_customer_id,
        brand_ambassador_id
    FROM {{ref('redaspen_distributors')}}
)
SELECT DISTINCT(oi.id) AS order_id,
    oi.order_number,
    olc.line_item_subtotal*100 AS subtotal_amount_cents,
    tlc.total_tax_amount*100 AS sales_tax_amount_cents,
    sa.total_shipping_amount*100 AS shipping_amount_cents,
    sta.shipping_tax_amount*100 AS shipping_tax_amount_cents,
    od.total_discount_amount*100 AS total_discount_amount_cents,
    oi.order_invoice*100 AS order_invoice_amount_cents,
    DATEADD(hour, -7, o.created_at)::timestamp_ntz AS created_at,
    DATEADD(hour, -7, o.processed_at)::timestamp_ntz AS processed_at,
    DATEADD(hour, -7, o.cancelled_at)::timestamp_ntz AS cancelled_at,
    DATEADD(hour, -7, o.updated_at)::timestamp_ntz AS updated_at,
    DATE_TRUNC('month', created_at) AS bonus_period,
    o.cancel_reason,
    o.financial_status,
    o.fulfillment_status,
    ABS(orf.subtotal_refund) AS subtotal_refund_cents,
    ABS(orf.total_tax_refund) AS sales_tax_refund_cents,
    ABS(oac.shipping_refund)*100 AS shipping_refund_cents,
    ABS(oac.shipping_tax_refund)*100 AS shipping_tax_refund_cents,
    oac.order_adjustment_amount*100 AS order_adjustment_amount_cents,
    oac.order_adjustment_tax_amount*100 AS order_adjustment_tax_amount_cents,
    oi.order_refund*100 AS order_refund_amount_cents,
    (order_invoice_amount_cents - order_refund_amount_cents)::number AS total_order_amount_cents,
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
    o.checkout_token,
    o.referring_site,
    o.app_id,
    o.buyer_accepts_marketing,
    otc.order_tag_type,
    REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"SponsorID"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1) AS sponsor_id,
    REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"PartyID"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1) AS partyid,
    REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"HostID"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1) AS hostid,
    REGEXP_SUBSTR(o.note_attributes, '"name"\s*:\s*"dateOfBirth"\s*,\s*"order_id"\s*:\s*null\s*,\s*"value"\s*:\s*"([^"]*)"', 1, 1, 'i', 1) AS dateofbirth,
    o.note_attributes,
    --Edit the splits once I find out more information on how notes/comments are inputted
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
    LEFT JOIN order_adjustment_cond oac ON oi.id = oac.id
    LEFT JOIN order_tag_cond otc ON oi.id = otc.order_id
    LEFT JOIN distributor_status_metafield dsm ON oi.id = dsm.order_id
    LEFT JOIN {{ source('shopify_raw', '"ORDER"') }} o ON oi.id = o.id
    LEFT JOIN customers c ON o.customer_id = c.shopify_customer_id
WHERE o._fivetran_deleted = FALSE
    AND o.test = FALSE







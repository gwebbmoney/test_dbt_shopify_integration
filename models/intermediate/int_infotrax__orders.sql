-- Creates view that grabs all infotrax orders
WITH sales_information AS(SELECT *
                    FROM {{ ref('stg_infotrax__orders') }}
                    WHERE ORDER_SOURCE <> 904
-- Grabs all infotrax orders
-- These orders do not include refunded orders
),
refund_information AS(SELECT *
                    FROM {{ ref('stg_infotrax__orders') }}
                    WHERE ORDER_SOURCE = 904
-- Grabs all infotrax refunded orders
-- These orders only contain refunded orders
),
refund_cond AS(SELECT ri.infotrax_original_order,
    ri.order_source,
    SUM(ri.retail_amount_cents) AS retail_amount_cents,
    SUM(ri.discount_amount_cents) AS discount_amount_cents,
    SUM(ri.sales_tax_cents) AS sales_tax_cents,
    SUM(ri.freight_amount_cents) AS freight_amount_cents,
    SUM(ri.total_invoice_cents) AS total_invoice_cents,
    SUM(ri.pv_qualifying_amount_cents) AS pv_qualifying_amount_cents
FROM refund_information ri
WHERE order_status <> 9
GROUP BY infotrax_original_order, ri.order_source
-- Creates fields that contain order information
),
orders_comb AS(SELECT si.*,
    IFNULL(rc.retail_amount_cents,0) AS refund_subtotal_amount,
    IFNULL(rc.discount_amount_cents,0) AS refund_discount_amount,
    IFNULL(rc.sales_tax_cents,0) AS refund_sales_tax_amount,
    IFNULL(rc.freight_amount_cents,0) AS refund_freight_amount,
    IFNULL(rc.total_invoice_cents,0) AS refund_invoice_amount,
    IFNULL(rc.pv_qualifying_amount_cents, 0) AS refund_pv_qualifying_amount,
    rc.order_source AS refund_order_source
FROM sales_information si LEFT JOIN refund_cond rc ON si.infotrax_order_number = rc.infotrax_original_order
-- Creates fields that contains refunded order information and combines them with order information
-- Used to match how Shopify calculates orders
),
order_integration AS(SELECT infotrax_order_number,
    retail_amount_cents,
    sales_tax_cents,
    discount_amount_cents,
    freight_amount_cents,
    total_invoice_cents,
    bonus_period,
    entered_at,
    posted_at,
    updated_at,
    (CASE WHEN refund_order_source IS NOT NULL THEN refund_order_source
        ELSE order_source
    END) AS order_source,
    order_status,
    refund_subtotal_amount,
    refund_discount_amount,
    refund_sales_tax_amount,
    refund_freight_amount,
    refund_invoice_amount,
    refund_pv_qualifying_amount,
    (total_invoice_cents - refund_invoice_amount) AS total_order_amount_cents,
    ship_to_name,
    ship_to_addr_1,
    ship_to_addr_2,
    ship_to_city,
    ship_to_state,
    ship_to_zip,
    distributor_id,
    distributor_status
FROM orders_comb oc
-- Combines orders and refunded orders used to match to Shopify
)
SELECT oi.infotrax_order_number AS order_id,
    retail_amount_cents AS subtotal_amount_cents,
    sales_tax_cents AS sales_tax_amount_cents,
    discount_amount_cents AS total_discount_amount_cents,
    freight_amount_cents AS shipping_amount_cents,
    total_invoice_cents AS order_invoice_amount_cents,
    bonus_period,
    entered_at AS created_at,
    posted_at AS processed_at,
    updated_at,
    (CASE 
        WHEN order_source = 904 AND total_order_amount_cents = 0 THEN 'refunded'
        WHEN order_source = 904 AND total_order_amount_cents > 0 THEN 'partially_refunded'
        ELSE 'paid'
     END) AS financial_status,
     -- Shopify field that states the status of an order
    (CASE
        WHEN oi.order_status = 1 THEN 'unfulfilled'
        WHEN oi.order_status = 9 THEN 'cancelled'
        ELSE 'fulfilled'
     END) AS fulfillment_status,
     -- Shopify field that states the shipping status of an order
    (CASE
        WHEN order_source = 903 THEN ARRAY_CONSTRUCT('Subscription_Order')
        WHEN order_source = 905 THEN ARRAY_CONSTRUCT('Enrollment_Order')
        ELSE []
    END) AS order_tag_type,
    -- Shopify field that houses the subscription type of the order or if the order was an enrollment kit
    (CASE
        WHEN order_source = 915 THEN TRUE
        ELSE FALSE
    END) AS redeemed_pop_up_reward,
    -- Boolean that states if a pop up reward was used
    refund_subtotal_amount AS subtotal_refund_cents,
    refund_discount_amount AS discount_refund_cents,
    refund_sales_tax_amount AS sales_tax_refund_cents,
    refund_freight_amount AS shipping_refund_cents,
    refund_invoice_amount AS order_refund_amount_cents,
    refund_pv_qualifying_amount AS pv_qualifying_amount_refund_cents,
    total_order_amount_cents,
    ship_to_name AS shipping_address_name,
    ship_to_addr_1 AS shipping_address_one,
    ship_to_addr_2 AS shipping_address_two,
    ship_to_city AS shipping_address_city,
    ship_to_state AS shipping_address_state,
    ship_to_zip AS shipping_address_zip,
    distributor_id AS brand_ambassador_id,
    distributor_status
FROM order_integration oi
-- Organizes view into it's final format

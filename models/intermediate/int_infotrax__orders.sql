WITH sales_information AS(SELECT *
                    FROM {{ ref('stg_infotrax__orders') }}
                    WHERE ORDER_SOURCE <> 904
),
refund_information AS(SELECT *
                    FROM {{ ref('stg_infotrax__orders') }}
                    WHERE ORDER_SOURCE = 904
),
refund_cond AS(SELECT ri.infotrax_original_order,
    ri.order_source,
    SUM(ri.retail_amount_cents) AS retail_amount_cents,
    SUM(ri.discount_amount_cents) AS discount_amount_cents,
    SUM(ri.sales_tax_cents) AS sales_tax_cents,
    SUM(ri.freight_amount_cents) AS freight_amount_cents,
    SUM(ri.total_invoice_cents) AS total_invoice_cents
FROM refund_information ri
WHERE order_status <> 9
GROUP BY infotrax_original_order, ri.order_source
),
orders_comb AS(SELECT si.*,
    IFNULL(rc.retail_amount_cents,0) AS refund_subtotal_amount,
    IFNULL(rc.discount_amount_cents,0) AS refund_discount_amount,
    IFNULL(rc.sales_tax_cents,0) AS refund_sales_tax_amount,
    IFNULL(rc.freight_amount_cents,0) AS refund_freight_amount,
    IFNULL(rc.total_invoice_cents,0) AS refund_invoice_amount,
    rc.order_source AS refund_order_source
FROM sales_information si LEFT JOIN refund_cond rc ON si.infotrax_order_number = rc.infotrax_original_order
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
    (CASE
        WHEN oi.order_status = 1 THEN 'unfulfilled'
        WHEN oi.order_status = 9 THEN 'cancelled'
        ELSE 'fulfilled'
     END) AS fulfillment_status,
    ri.refund_subtotal_amount AS subtotal_refund_cents,
    ri.refund_discount_amount AS discount_refund_cents,
    ri.refund_sales_tax_amount AS sales_tax_refund_cents,
    ri.refund_freight_amount AS shipping_refund_cents,
    ri.refund_invoice_amount AS order_refund_amount_cents,
    total_order_amount_cents,
    ship_to_name AS shipping_address_name,
    ship_to_addr_1 AS shipping_address_one,
    ship_to_addr_2 AS shipping_address_two,
    ship_to_city AS shipping_address_city,
    ship_to_state AS shipping_address_state,
    ship_to_zip AS shipping_address_zip,
    distributor_id AS brandambassadorid,
    distributor_status
FROM order_integration oi


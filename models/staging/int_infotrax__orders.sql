WITH sales_information AS(SELECT *
                    FROM {{ source('redaspen_data', 'ORDERS') }}
                    WHERE ORDER_SOURCE <> 904
),
refund_information AS(SELECT *
                    FROM {{ source('redaspen_data', 'ORDERS') }}
                    WHERE ORDER_SOURCE = 904
),
orders_comb AS(SELECT si.*,
    IFNULL(ri.retail_amount_cents,0) AS refund_subtotal_amount,
    IFNULL(ri.discount_amount_cents,0) AS refund_discount_amount,
    IFNULL(ri.sales_tax_cents,0) AS refund_sales_tax_amount,
    IFNULL(ri.freight_amount_cents,0) AS refund_freight_amount,
    IFNULL(ri.total_invoice_cents,0) AS refund_invoice_amount,
    ri.entered_at AS refund_date,
    ri.order_source AS refund_order_source
FROM sales_information si LEFT JOIN refund_information ri ON si.infotrax_order_number = ri.infotrax_original_order
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
SELECT infotrax_order_number AS order_number,
    retail_amount_cents,
    sales_tax_cents,
    discount_amount_cents,
    freight_amount_cents,
    total_invoice_cents,
    bonus_period,
    entered_at,
    posted_at,
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
    refund_subtotal_amount,
    refund_discount_amount,
    refund_sales_tax_amount,
    refund_freight_amount,
    refund_invoice_amount,
    total_order_amount_cents,
    ship_to_name,
    ship_to_addr_1,
    ship_to_addr_2,
    ship_to_city,
    ship_to_state,
    ship_to_zip,
    odp.billing_first_name,
    odp.billing_last_name,
    odp.credit_card_name,
    odp.billing_addr1,
    odp.billing_addr2,
    odp.billing_city,
    odp.billing_state,
    odp.billing_zip,
    distributor_id,
    distributor_status
FROM order_integration oi LEFT JOIN {{ source('raw_infotrax', 'ORDERPAYMENTS') }} odp ON oi.infotrax_order_number = odp.order_number
WITH sales_information AS(SELECT *
                    FROM {{ ref('stg_infotrax__orders') }}
                    WHERE ORDER_SOURCE <> 904
),
refund_information AS(SELECT *
                    FROM {{ ref('stg_infotrax__orders') }}
                    WHERE ORDER_SOURCE = 904
),
order_refund AS(SELECT si.*,
    IFNULL(ri.retail_amount_cents,0) AS refund_subtotal_amount,
    IFNULL(ri.discount_amount_cents,0) AS refund_discount_amount,
    IFNULL(ri.sales_tax_cents,0) AS refund_sales_tax_amount,
    IFNULL(ri.freight_amount_cents,0) AS refund_freight_amount,
    IFNULL(ri.total_invoice_cents,0) AS refund_invoice_amount,
    ri.infotrax_order_number AS infotrax_order_refund_number,
    ri.entered_at::timestamp_ntz AS refunded_at,
    ri.posted_at::timestamp_ntz AS processed_at,
    ri.bonus_period AS refund_bonus_period,
    ri.order_source AS refund_order_status
FROM sales_information si LEFT JOIN refund_information ri ON si.infotrax_order_number = ri.infotrax_original_order
)
SELECT *
FROM order_refund
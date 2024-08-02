-- Creates a view for order line refunds
WITH order_line_refund AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_LINE_REFUND') }}
-- Grabs all of the order line refund information
),
order_lines AS(
    SELECT * FROM {{ ref("int_shopify__order_lines") }}
-- Grabs all of the order line information
-- Used to link order line refunds to it's original order id
)
SELECT ol.order_id,
    olr.order_line_id,
    olr.refund_id,
    ol.sku,
    ol.shopify_product_id AS product_id,
    ol.product_name,
    olr.restock_type,
    olr.subtotal * 100 AS refund_price_cents,
    olr.quantity AS refund_quantity,
    olr.total_tax * 100 AS refund_tax_cents,
    CAST((refund_price_cents * refund_quantity) AS NUMBER) AS pre_tax_refund_cents,
    ol.bundle_properties
FROM order_line_refund olr LEFT JOIN order_lines ol ON olr.order_line_id = ol.order_line_id
-- Organizes order line refund into it's final format
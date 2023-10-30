WITH order_lines AS(SELECT ol.*,
                    sk.skuable_type
                FROM {{ ref("stg_infotrax__order_lines") }} ol LEFT JOIN {{ source('redaspen', 'SKUS')}} sk ON ol.infotrax_sku = sk.name
                WHERE INFOTRAX_SKU <> 'Discount'
                    AND INFOTRAX_SKU <> 'HOSTCREDIT'
),
order_lines_cond AS(SELECT ol.*
                FROM order_lines ol JOIN {{ ref("stg_infotrax__orders") }} o ON ol.infotrax_order_number = o.infotrax_order_number
                WHERE o.order_source = 904
),
bundle_order_lines AS(SELECT *
                FROM {{ ref("stg_infotrax__bundle_refund_order_lines") }}
),
bundle_lines AS(SELECT infotrax_order_number,
            infotrax_original_order,
            order_line_id,
            bundle_product_number,
            product_name,
            retail_amount_cents,
            quantity_returned,
            line_item_price_cents,
            order_line,
            promo_id,
            component_status
        FROM bundle_order_lines
        WHERE kit_line = 0
),
product_bundle_array AS(SELECT DISTINCT(bol.order_line_id),
                            bol.infotrax_original_order,
                            bol.order_line AS product_order_line,
                            bl.order_line AS bundle_order_line,
                        ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('set',bl.bundle_product_number, 
                                            'set_name', bl.product_name, 
                                            'price', bl.retail_amount_cents, 
                                            'quantity', bl.quantity_returned, 
                                            'total_amount', bl.line_item_price_cents, 
                                            'order_line_id', bl.order_line_id,
                                            'infotrax_order_number', bl.infotrax_order_number)) AS properties
        FROM bundle_lines bl JOIN bundle_order_lines bol ON bl.infotrax_order_number = bol.infotrax_order_number
            AND bl.bundle_product_number = bol.bundle_product_number AND bl.promo_id = bol.promo_id
            AND ABS(bol.order_line - bl.order_line) = bol.kit_line
            AND bol.component_status NOT IN ('M', 'P')
        ORDER BY bol.order_line_id
),
bundle_properties AS(SELECT bol.*,
    pba.properties
FROM product_bundle_array pba JOIN bundle_order_lines bol ON pba.order_line_id = bol.order_line_id
    AND bol.infotrax_order_number = pba.infotrax_order_number AND bol.order_line = pba.product_order_line
),
product_order_line AS(SELECT olc.*,
    bp.emma_price_cents,
    bp.bundle_product_allocation_revenue_cents,
    bp.properties
FROM order_lines_cond olc LEFT JOIN bundle_properties bp ON olc.id = bp.order_line_id
    AND olc.infotrax_order_number = bp.infotrax_order_number
WHERE olc.component_status NOT IN ('M', 'P')
)
SELECT id AS order_line_id,
    infotrax_original_order AS order_id,
    product_id,
    product_name,
    infotrax_sku AS sku,
    order_line AS refund_order_line,
    properties,
    (CASE
        WHEN properties is not null AND kit_line > 0 THEN emma_price_cents
        ELSE retail_amount_cents
    END) AS refund_price_cents,
    quantity_returned AS refund_quantity,
    (refund_price_cents * refund_quantity) AS line_item_price_cents,
    (CASE
        WHEN kit_line > 0 THEN bundle_product_allocation_revenue_cents
        ELSE line_item_price_cents
    END) AS pre_tax_refund_cents
FROM product_order_line
ORDER BY infotrax_order_number, order_line

WITH order_lines AS(SELECT ol.*,
                    sk.skuable_type
                FROM {{ ref("stg_infotrax__order_lines") }} ol LEFT JOIN {{ source('redaspen', 'SKUS')}} sk ON ol.infotrax_sku = sk.name
                WHERE INFOTRAX_SKU <> 'Discount'
                    AND INFOTRAX_SKU <> 'HOSTCREDIT'
),
order_lines_cond AS(SELECT ol.*
                FROM order_lines ol JOIN {{ ref("stg_infotrax__orders") }} o ON ol.infotrax_order_number = o.infotrax_order_number
                WHERE o.order_source <> 904
),
bundle_order_lines AS(SELECT *
                FROM {{ ref("stg_infotrax__bundle_order_lines") }}
),
bundle_lines AS(SELECT infotrax_order_number,
            order_line_id,
            bundle_product_number,
            product_name,
            retail_amount_cents,
            quantity_ordered,
            line_item_price_cents,
            order_line,
            promo_id,
            component_status
        FROM bundle_order_lines
        WHERE kit_line = 0
            AND component_status IN ('M', 'P')
),
product_bundle_array AS(SELECT DISTINCT(bol.order_line_id),
                            bol.infotrax_order_number,
                            bol.order_line AS product_order_line,
                            bl.order_line AS bundle_order_line,
                        ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('set',bl.bundle_product_number, 
                                            'set_name', bl.product_name, 
                                            'price', bl.retail_amount_cents, 
                                            'quantity', bl.quantity_ordered, 
                                            'total_amount', bl.line_item_price_cents, 
                                            'bundle_order_line_id', bl.order_line_id,
                                            'infotrax_order_number', bl.infotrax_order_number,
                                            'product_order_line_id', bol.order_line_id)) AS properties
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
SELECT pol.id AS order_line_id,
    pol.infotrax_order_number AS order_id,
    pol.product_id AS emma_product_id,
    (CASE
        WHEN p.emma_product_id = pol.product_id AND pol.skuable_type = 'Product' THEN p.shopify_product_id
        WHEN b.emma_bundle_id = pol.product_id AND pol.skuable_type = 'Bundle' THEN b.shopify_bundle_id
    END) AS shopify_product_id,
    pol.product_name,
    pol.infotrax_sku AS sku,
    pol.order_line,
    pol.properties,
    (CASE
        WHEN pol.properties is not null AND pol.kit_line > 0 THEN emma_price_cents
        ELSE pol.retail_amount_cents
    END) AS price_cents,
    pol.quantity_ordered,
    (price_cents * quantity_ordered) AS line_item_price_cents,
    (CASE
        WHEN pol.kit_line > 0 THEN pol.bundle_product_allocation_revenue_cents
        ELSE line_item_price_cents
    END) AS pre_tax_price_cents,
    (line_item_price_cents - pre_tax_price_cents) AS total_discount_cents,
    pol.skuable_type
FROM product_order_line pol LEFT JOIN {{ ref("redaspen_product_variants") }} p ON pol.infotrax_sku = p.sku
    LEFT JOIN {{ ref("redaspen_bundle_variants") }} b ON pol.infotrax_sku = b.sku
ORDER BY pol.infotrax_order_number, pol.order_line

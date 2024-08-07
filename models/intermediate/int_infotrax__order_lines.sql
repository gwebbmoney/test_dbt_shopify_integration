-- Creates view that houses infotrax order lines and transforms this data in order to combine to Shopify
WITH order_lines AS(SELECT ol.*,
                    sk.skuable_type
                FROM {{ ref("stg_infotrax__order_lines") }} ol LEFT JOIN {{ source('redaspen', 'SKUS')}} sk ON ol.infotrax_sku = sk.name
                WHERE INFOTRAX_SKU <> 'Discount'
                    AND INFOTRAX_SKU <> 'HOSTCREDIT'
-- Grabs order lines that do not contain 'Discount' or 'HOSTCREDIT' rows
),
order_lines_cond AS(SELECT ol.*
                FROM order_lines ol JOIN {{ ref("stg_infotrax__orders") }} o ON ol.infotrax_order_number = o.infotrax_order_number
                WHERE o.order_source <> 904
-- Grabs all order lines that were not within a refunded order
),
bundle_order_lines AS(SELECT *
                FROM {{ ref("stg_infotrax__bundle_order_lines") }}
-- Grabs data from bundle order lines
-- Used to help match the Shopify format on how they calculate/display bundle information
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
-- Grab all bundles within the bundle order lines table
-- Component status = M and P means is an Infotrax field. Basically states if the product shown was a bundle or not.
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
        WHERE ABS(bol.order_line - bl.order_line) = bol.kit_line
            AND bol.order_line > bl.order_line
            AND bol.component_status NOT IN ('M', 'P')
        ORDER BY bol.order_line_id
-- Grabs all products that were within a bundle
-- Creates array object to house all of the bundle information
),
bundle_properties AS(SELECT bol.*,
    pba.properties
FROM product_bundle_array pba JOIN bundle_order_lines bol ON pba.order_line_id = bol.order_line_id
    AND bol.infotrax_order_number = pba.infotrax_order_number AND bol.order_line = pba.product_order_line
-- Combines product bundle array with bundle order lines
),
product_order_line AS(SELECT olc.*,
    bp.emma_price_cents,
    bp.bundle_product_allocation_revenue_cents,
    bp.properties
FROM order_lines_cond olc LEFT JOIN bundle_properties bp ON olc.id = bp.order_line_id
    AND olc.infotrax_order_number = bp.infotrax_order_number
WHERE olc.component_status NOT IN ('M', 'P')
-- Grabs the product order line of a product
)
SELECT pol.id AS order_line_id,
    pol.infotrax_order_number AS order_id,
    pol.product_id AS emma_product_id,
    pol.product_name,
    pol.infotrax_sku AS sku,
    pol.order_line,
    pol.properties AS bundle_properties,
    (CASE
        WHEN bundle_properties is not null AND pol.kit_line > 0 THEN emma_price_cents
        ELSE pol.retail_amount_cents
    END) AS price_cents,
    pol.quantity_ordered,
    (price_cents * quantity_ordered) AS line_item_price_cents,
    (CASE
        WHEN pol.kit_line > 0 THEN line_item_price_cents - pol.bundle_product_allocation_revenue_cents
        ELSE 0
    END) AS bundle_discount_cents,
    (CASE
        WHEN pol.kit_line > 0 THEN pol.bundle_product_allocation_revenue_cents
        ELSE line_item_price_cents
    END) AS subtotal_price_cents,
    (CASE
        WHEN pol.kit_line > 0 THEN pol.bundle_product_allocation_revenue_cents
        ELSE line_item_price_cents
    END) AS pre_tax_price_cents,
    (subtotal_price_cents - pre_tax_price_cents) AS line_item_order_discount_cents,
    pol.skuable_type,
    pol.distributor_status
FROM product_order_line pol
ORDER BY pol.infotrax_order_number, pol.order_line
-- Organizes view into it's final format





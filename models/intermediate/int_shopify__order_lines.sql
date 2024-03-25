WITH order_lines AS(
    SELECT * FROM {{ source('shopify_raw', 'ORDER_LINE') }}
),
loyalty_box_discount AS(
    SELECT STRTOK_TO_ARRAY(dap.title, '-') AS loyalty_box_array,
        ol.order_id,
        ol.id,
        ol.sku
    FROM {{ source('shopify_raw', 'DISCOUNT_ALLOCATION') }} dal JOIN order_lines ol ON dal.order_line_id = ol.id
        JOIN {{ source('shopify_raw', 'DISCOUNT_APPLICATION') }} dap ON dap.order_id = ol.order_id AND dap.index = dal.discount_application_index
),
bundle_discounts AS(
    SELECT dap.order_id,
        dal.order_line_id,
        dal.discount_application_index,
        SUM(dal.amount)*100 AS bundle_discount
    FROM {{ source('shopify_raw', 'DISCOUNT_ALLOCATION') }} dal JOIN order_lines ol ON dal.order_line_id = ol.id
        JOIN {{ source('shopify_raw', 'DISCOUNT_APPLICATION') }} dap ON dap.order_id = ol.order_id AND dap.index = dal.discount_application_index
    WHERE dap.target_selection = 'explicit'
        AND dap.target_type = 'line_item'
        AND dap.allocation_method = 'across'
    GROUP BY dap.order_id, dal.order_line_id, dal.discount_application_index
),
loyalty_box_sku AS(
    SELECT LTRIM(loyalty_box_array[1]::string) AS loyalty_box_sku,
        order_id,
        id,
        sku
    FROM loyalty_box_discount
),
bundles AS(
    SELECT *
    FROM {{ ref("redaspen_bundle_variants") }}
),
products AS(
    SELECT *
    FROM {{ ref("redaspen_product_variants") }}
),
loyalty_box_object AS(SELECT ls.id,
    ls.order_id,
    ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('loyalty_box_order_id', ol.order_id::number,
    'loyalty_box_order_line_id', ol.id::number,
    'loyalty_box_sku',ol.sku::string,
    'loyalty_box_title', ol.title::string,
    'loyalty_box_order_quantity', ol.quantity::number,
    'loyalty_box_price', ol.price::number,
    'loyalty_box_subtotal', (ol.price*ol.quantity)::number,
    'loyalty_box_total', ol.pre_tax_price::number)) AS loyalty_box_properties
    FROM loyalty_box_sku ls JOIN order_lines ol ON ls.order_id = ol.order_id AND ls.loyalty_box_sku = ol.sku
),
norm_order_lines AS(SELECT id AS order_line_id,
    order_id,
    product_id AS shopify_product_id,
    (CASE
        WHEN p.shopify_product_id = ol.product_id THEN p.emma_product_id
        WHEN b.shopify_bundle_id = ol.product_id THEN b.emma_id
    END) AS emma_product_id,
    variant_id AS product_variant_id,
    title AS product_name,
    variant_title AS product_variant_name,
    ol.sku,
    nullif(properties, []) AS bundle_properties,
    index AS order_line,
    (CASE
        WHEN p.shopify_product_id = ol.product_id THEN 'Product'
        WHEN b.shopify_bundle_id = ol.product_id THEN 'Bundle'
    END) AS skuable_type,
    bundle_type,
    p.product_tag,
    ol.price*100 AS price_cents,
    quantity AS quantity_ordered,
    fulfillable_quantity,
    (price_cents * quantity) AS line_item_price_cents,
    total_discount*100 AS total_discount_cents,
    pre_tax_price*100 AS pre_tax_price_cents,
    gift_card
FROM order_lines ol LEFT JOIN products p ON ol.sku = p.sku AND ol.variant_id = p.shopify_product_variant_id 
    LEFT JOIN bundles b ON ol.sku = b.sku AND ol.variant_id = b.shopify_bundle_variant_id
WHERE b.bundle_type IS NULL OR b.bundle_type IN ('Bundle_Fixed', 'Bundle', 'Bundle_Custom')
),
order_lines_rough AS(SELECT nol.order_line_id,
    nol.order_id,
    nol.shopify_product_id,
    nol.emma_product_id,
    nol.product_variant_id,
    nol.product_name,
    nol.product_variant_name,
    nol.sku,
    COALESCE(nol.bundle_properties, lbo.loyalty_box_properties) AS bundle_properties,
    nol.order_line,
    nol.bundle_type,
    nol.product_tag,
    nol.price_cents,
    nol.quantity_ordered,
    nol.fulfillable_quantity,
    nol.line_item_price_cents,
    nol.total_discount_cents,
    nol.pre_tax_price_cents,
    nol.gift_card
FROM norm_order_lines nol LEFT JOIN loyalty_box_object lbo ON nol.order_line_id = lbo.id AND nol.order_id = lbo.order_id
),
loyalty_box_join AS(SELECT order_id,
                        bundle_properties,
                        SUM(line_item_price_cents) AS loyalty_box_product_price
                    FROM order_lines_rough
                    WHERE bundle_properties[0]['loyalty_box_order_id'] IS NOT NULL
                    GROUP BY order_id, bundle_properties
),
percentage_allocation AS(SELECT ol.order_line_id,
                            ol.order_id,
                            ol.line_item_price_cents,
                            ol.bundle_properties,
                            DIV0(ol.line_item_price_cents, lbj.loyalty_box_product_price) AS allocated_percentage, --Used to calculate product price on the line item level for loyalty box purchases
                            (DIV0(ol.line_item_price_cents, lbj.loyalty_box_product_price) * ol.bundle_properties[0]['loyalty_box_subtotal']) AS subtotal_price,
                            (DIV0(ol.line_item_price_cents, lbj.loyalty_box_product_price) * ol.bundle_properties[0]['loyalty_box_total']) AS pre_tax_price
                    FROM loyalty_box_join lbj JOIN order_lines_rough ol ON lbj.order_id = ol.order_id 
                        AND lbj.bundle_properties = ol.bundle_properties
),
final AS(SELECT ol.order_line_id,
    ol.order_id,
    ol.shopify_product_id,
    ol.emma_product_id,
    ol.product_variant_id,
    ol.product_name,
    ol.product_variant_name,
    ol.sku,
    ol.bundle_properties,
    ol.order_line,
    ol.bundle_type,
    ol.product_tag,
    ol.price_cents,
    ol.quantity_ordered,
    ol.fulfillable_quantity,
    ol.line_item_price_cents,
    (CASE
        WHEN ol.bundle_properties[0]['loyalty_box_order_id'] IS NOT NULL THEN round(ol.line_item_price_cents - (pa.subtotal_price*100), 2)
        ELSE coalesce(bd.bundle_discount, 0)
    END) AS bundle_discount_cents,
    (CASE
        WHEN ol.bundle_properties[0]['loyalty_box_order_id'] IS NOT NULL THEN round(pa.subtotal_price*100, 2)
        ELSE ol.line_item_price_cents - bundle_discount_cents
    END) AS subtotal_price_cents,
    (CASE
        WHEN ol.bundle_properties[0]['loyalty_box_order_id'] IS NOT NULL THEN round(pa.pre_tax_price*100, 2)
        ELSE ol.pre_tax_price_cents
    END) AS pre_tax_price_cents_f,
    (CASE
        WHEN subtotal_price_cents is null THEN ol.line_item_price_cents - pre_tax_price_cents_f
        ELSE round(subtotal_price_cents - pre_tax_price_cents_f)
    END) AS line_item_order_discount_cents,
    ol.gift_card
FROM order_lines_rough ol LEFT JOIN percentage_allocation pa ON ol.order_line_id = pa.order_line_id
    AND ol.bundle_properties = pa.bundle_properties
LEFT JOIN bundle_discounts bd ON ol.order_line_id = bd.order_line_id
)
SELECT order_line_id,
    order_id,
    shopify_product_id,
    emma_product_id,
    product_variant_id,
    product_name,
    product_variant_name,
    sku,
    bundle_properties,
    order_line,
    bundle_type,
    product_tag,
    price_cents,
    quantity_ordered,
    fulfillable_quantity,
    line_item_price_cents,
    bundle_discount_cents,
    subtotal_price_cents,
    line_item_order_discount_cents,
    pre_tax_price_cents_f AS pre_tax_price_cents,
    gift_card
FROM final
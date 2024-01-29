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
loyalty_box_sku AS(
    SELECT LTRIM(loyalty_box_array[1]::string) AS loyalty_box_sku,
        order_id,
        id,
        sku
    FROM loyalty_box_discount
),
loyalty_box_object AS(SELECT ls.id,
    ls.order_id,
    ARRAY_CONSTRUCT(OBJECT_CONSTRUCT('loyalty_box_order_id', ol.order_id::number,
    'loyalty_box_order_line_id', ol.id::number,
    'loyalty_box_sku',ol.sku::string,
    'loyalty_box_title', ol.title::string,
    'loyalty_box_order_quantity', ol.quantity::number,
    'loyalty_box_price', ol.price::number,
    'loyalty_box_total', ol.pre_tax_price::number)) AS loyalty_box_properties
    FROM loyalty_box_sku ls JOIN order_lines ol ON ls.order_id = ol.order_id AND ls.loyalty_box_sku = ol.sku
),
norm_order_lines AS(SELECT id AS order_line_id,
    order_id,
    product_id AS shopify_product_id,
    (CASE
        WHEN p.shopify_product_id = ol.product_id THEN p.emma_product_id
        WHEN b.shopify_bundle_id = ol.product_id THEN b.emma_bundle_id
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
FROM order_lines ol LEFT JOIN {{ ref("redaspen_product_variants") }} p ON ol.sku = p.sku AND ol.variant_id = p.shopify_product_variant_id -- Change to just sku join once duplicate products are eliminated
    LEFT JOIN {{ ref("redaspen_bundle_variants") }} b ON ol.sku = b.sku AND ol.variant_id = b.shopify_bundle_variant_id
WHERE bundle_type <> 'LoyaltyBox' -- All loyalty boxes will be within bundle properties
)
SELECT nol.order_line_id,   
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
    nol.gift_card,
    nol.source
FROM norm_order_lines nol LEFT JOIN loyalty_box_object lbo ON nol.order_line_id = lbo.id AND nol.order_id = lbo.order_id

    
WITH 
orders AS(SELECT *
        FROM {{ ref("stg_infotrax__orders") }}
        WHERE order_source <> 904
),
bundle_product_orders AS(SELECT ol.infotrax_order_number,
                        ol.id,
                        ol.product_name,
                        ol.infotrax_sku,
                        ol.quantity_ordered,
                        ol.quantity_returned,
                        ol.order_line,
                        ol.retail_amount_cents,
                        ol.promo_id,
                        ol.kit_line,
                        DATE(o.entered_at) AS entered_at,
                        o.distributor_status,
                        sk.skuable_type,
                        ol.component_status
                FROM {{ ref("stg_infotrax__order_lines") }} ol LEFT JOIN {{ source('redaspen', 'SKUS') }} sk ON ol.infotrax_sku = sk.name
                    JOIN orders o ON ol.infotrax_order_number = o.infotrax_order_number
                WHERE sk.skuable_type = 'Bundle' OR ol.component_status IN ('M', 'P')
                    AND ol.kit_line = 0
                    AND o.bonus_period >= '2020-01-01'
                UNION 
                    SELECT ol.infotrax_order_number,
                        ol.id,
                        ol.product_name,
                        ol.infotrax_sku,
                        ol.quantity_ordered,
                        ol.quantity_returned,
                        ol.order_line,
                        ol.retail_amount_cents,
                        ol.promo_id,
                        ol.kit_line,
                        DATE(o.entered_at) AS entered_at,
                        o.distributor_status,
                        sk.skuable_type,
                        ol.component_status
                FROM {{ ref("stg_infotrax__order_lines") }} ol LEFT JOIN {{ source('redaspen', 'SKUS') }} sk ON ol.infotrax_sku = sk.name
                    JOIN orders o ON ol.infotrax_order_number = o.infotrax_order_number
                WHERE ol.kit_line > 0
                    AND o.bonus_period >= '2020-01-01'
),
product_suggested_price AS(SELECT p.id,
                        sk.name AS sku,
                        p.price
                    FROM {{ source("redaspen", "PRODUCTS") }} p LEFT JOIN {{ source("redaspen", "SKUS") }} sk ON p.id = sk.skuable_id
                    WHERE sk.skuable_type = 'Product'
),
bundle_suggested_price AS(SELECT b.id,
                            sk.name AS sku,
                            b.price
                    FROM {{ source("redaspen", "BUNDLES") }} b LEFT JOIN {{ source("redaspen", "SKUS") }} sk ON b.id = sk.skuable_id
                    WHERE sk.skuable_type = 'Bundle'
),
bundle_product_lines AS(SELECT bpo.infotrax_order_number,
    bpo.id,
    bpo.component_status,
    bpo.infotrax_sku as sku,
    bpo.product_name,
    bpo.quantity_ordered,
    bpo.quantity_returned,
    bpo.retail_amount_cents,
    (CASE 
        WHEN bpo.promo_id IS NULL THEN '0'
        ELSE bpo.promo_id END) AS promo_id,
    (CASE
        WHEN bpo.skuable_type = 'Bundle' AND bsg.price IS NULL THEN 0
        WHEN bpo.skuable_type = 'Product' AND psg.price IS NULL THEN 0
        WHEN bpo.skuable_type = 'Product' THEN psg.price
        WHEN bpo.skuable_type = 'Bundle' THEN bsg.price
    END) AS emma_price_dollars,
    (CASE
        WHEN kit_line > 0 THEN emma_price_dollars * bpo.quantity_ordered
        WHEN kit_line = 0 THEN retail_amount_cents/100 * bpo.quantity_ordered
    END) AS suggested_price_dollars,
    bpo.kit_line,
    bpo.order_line,
    (CASE
        WHEN bpo.kit_line = 0 AND bpo.skuable_type = 'Bundle' THEN bpo.infotrax_sku
        WHEN bpo.kit_line = 0 AND bpo.skuable_type = 'Product' THEN bpo.infotrax_sku
    END) AS bundle_sku,
    bpo.skuable_type,
    bpo.distributor_status,
    bpo.entered_at
FROM bundle_product_orders bpo LEFT JOIN product_suggested_price psg ON bpo.infotrax_sku = psg.sku
    LEFT JOIN bundle_suggested_price bsg ON bpo.infotrax_sku = bsg.sku
ORDER BY bpo.infotrax_order_number,
    bpo.order_line
),
bundle_components AS(SELECT bpl.infotrax_order_number,
                    bpl.id,
                    bpl.kit_line,
                    bpl.order_line,
                    bpl.skuable_type,
                    bpl.suggested_price_dollars,
                    bpl.promo_id,
                (CASE WHEN bpl.kit_line > 0 THEN LAG(bundle_sku) IGNORE NULLS OVER (PARTITION BY infotrax_order_number ORDER BY order_line)
                    WHEN bpl.kit_line = 0 THEN bundle_sku
                END) AS bundle_product_number
FROM bundle_product_lines bpl
),
sum_allocation AS(SELECT (CASE 
                            WHEN it.product_line_sum_dollars IS NULL AND bc.skuable_type = 'Bundle' THEN bc.suggested_price_dollars
                            ELSE it.product_line_sum_dollars
                        END) AS product_line_sum_dollars,
                    bc.infotrax_order_number,
                    bc.bundle_product_number,
                    bc.order_line
                FROM (
                    SELECT SUM(COALESCE(bpl.suggested_price_dollars,0)) as product_line_sum_dollars,
                        bpl.infotrax_order_number,
                        bc.bundle_product_number,
                        bpl.promo_id
                    FROM bundle_product_lines bpl JOIN bundle_components bc ON bpl.infotrax_order_number = bc.infotrax_order_number
                        AND bpl.order_line = bc.order_line
                    WHERE bpl.kit_line > 0
                    GROUP BY bpl.infotrax_order_number, bc.bundle_product_number, bpl.promo_id
                    ) it RIGHT JOIN bundle_components bc ON it.infotrax_order_number = bc.infotrax_order_number
                        AND it.bundle_product_number = bc.bundle_product_number and it.promo_id = bc.promo_id 
),
price_allocation AS(SELECT bpl.infotrax_order_number,
    bpl.order_line,
    bpl.quantity_ordered,
    sa.product_line_sum_dollars,
    COALESCE((nullif(suggested_price_dollars,0) / nullif((product_line_sum_dollars),0)),0) AS product_price_allocation_percent
FROM bundle_product_lines bpl JOIN sum_allocation sa ON bpl.infotrax_order_number = sa.infotrax_order_number
    AND bpl.order_line = sa.order_line
),
bundle_order_line AS(SELECT bpl.infotrax_order_number,
                        bpl.id,
                        bpl.order_line,
                        bpl.kit_line,
                        bpl.emma_price_dollars,
                        bpl.suggested_price_dollars,
                        bpl.retail_amount_cents,
                        bc.bundle_product_number,
                        bpl.promo_id
                FROM bundle_product_lines bpl LEFT JOIN bundle_components bc ON bpl.infotrax_order_number = bc.infotrax_order_number
                    AND bpl.order_line = bc.order_line AND bpl.promo_id = bc.promo_id
                WHERE bpl.kit_line = 0
),
bundle_order_price_comp AS(SELECT DISTINCT(bc.id),
    bc.infotrax_order_number,
    bol.suggested_price_dollars,
    bc.order_line,
    bc.kit_line,
    bc.bundle_product_number, 
    (CASE
        WHEN bc.kit_line > 0 THEN ABS(bc.order_line - bol.order_line)
        WHEN bc.kit_line = 0 THEN 0
    END) AS absolute_value
FROM bundle_order_line bol RIGHT JOIN bundle_components bc ON bol.infotrax_order_number = bc.infotrax_order_number
    AND bol.bundle_product_number = bc.bundle_product_number AND bol.promo_id = bc.promo_id
WHERE absolute_value = bc.kit_line
),
bundle_order_price AS(
SELECT *,
    (CASE
        WHEN absolute_value = kit_line THEN suggested_price_dollars * 100
    END) AS bundle_line_price
FROM bundle_order_price_comp
),
product_pricing_percentages AS(SELECT bop.infotrax_order_number,
                            bop.order_line,
                            pa.product_line_sum_dollars,
                            pa.product_price_allocation_percent,
                            bop.bundle_product_number,
                            round((pa.product_price_allocation_percent * bop.bundle_line_price),2) AS bundle_product_allocation_price_cents
                        FROM price_allocation pa JOIN bundle_order_price bop ON pa.infotrax_order_number = bop.infotrax_order_number
                            AND pa.order_line = bop.order_line
),
bundle_order_lines AS(SELECT bpl.infotrax_order_number,
    bpl.id AS order_line_id,
    bpl.product_name,
    bpl.skuable_type,
    bpl.sku as line_item_sku,
    (CASE
        WHEN product_name LIKE '%Perk Pack%' THEN 0
        ELSE bpl.retail_amount_cents
    END) AS retail_amount_cents,
    (bpl.emma_price_dollars * 100) AS emma_price_cents,
    bpl.quantity_ordered,
    (CASE
        WHEN product_name LIKE '%Perk Pack%' THEN 0
        ELSE round(bpl.suggested_price_dollars * 100, 0)
    END)AS line_item_price_cents,
    (CASE
        WHEN kit_line = 0 THEN NULL
        WHEN kit_line > 0 THEN round(ppp.product_line_sum_dollars * 100, 0)
    END) AS order_line_sum_cents,
    (CASE 
        WHEN kit_line = 0 THEN NULL
        WHEN kit_line > 0 THEN round(ppp.product_price_allocation_percent, 2)
    END) AS price_allocation_percentage,
    (CASE
        WHEN sku IN (2836, 2744, 1835) AND kit_line = 0 THEN round(product_line_sum_dollars * 100, 0)
        WHEN bundle_product_number = 0 THEN 0
        WHEN kit_line = 0 THEN NULL 
        WHEN kit_line > 0 THEN ppp.bundle_product_allocation_price_cents
    END) AS bundle_product_allocation_revenue_cents,
    bpl.kit_line,
    bpl.order_line,
    ppp.bundle_product_number,
    bpl.promo_id,
    bpl.component_status,
    bpl.distributor_status,
    bpl.entered_at
FROM bundle_product_lines bpl JOIN product_pricing_percentages ppp ON bpl.infotrax_order_number = ppp.infotrax_order_number
    AND bpl.order_line = ppp.order_line
)
SELECT order_line_id,
    infotrax_order_number,
    line_item_sku,
    bundle_product_number,
    skuable_type,
    product_name,
    kit_line,
    order_line,
    distributor_status,
    entered_at,
    promo_id,
    retail_amount_cents,
    emma_price_cents,
    quantity_ordered,
    line_item_price_cents,
    order_line_sum_cents,
    price_allocation_percentage,
    bundle_product_allocation_revenue_cents,
    component_status
FROM bundle_order_lines
ORDER BY infotrax_order_number, order_line
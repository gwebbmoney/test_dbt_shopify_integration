WITH price_rule AS(SELECT id,
                    created_at,
                    starts_at,
                    ends_at,
                    title,
                    target_type,
                    target_selection,
                    allocation_method,
                    value_type,
                    value,
                    prerequisite_subtotal_range,
                    prerequisite_quantity_range,
                    prerequisite_shipping_price_range
                FROM {{ source('shopify_raw', 'PRICE_RULE') }}
),
discount_code AS(SELECT id,
                    price_rule_id,
                    code,    
                    usage_count
                FROM {{ source('shopify_raw', 'DISCOUNT_CODE') }}
)
SELECT dc.id AS discount_code_id,
    pr.id AS price_rule_id,
    dc.code AS discount_code,
    pr.title AS price_rule_title,
    pr.created_at,
    pr.starts_at,
    pr.ends_at,
    pr.target_type,
    pr.target_selection,
    pr.allocation_method,
    prerequisite_subtotal_range,
    prerequisite_quantity_range,
    prerequisite_shipping_price_range,
    pr.value_type,
    pr.value*100 AS value_cents,
    dc.usage_count AS order_count --Maybe don't include this, but keep for now
FROM price_rule pr JOIN discount_code dc ON pr.id = dc.price_rule_id
);
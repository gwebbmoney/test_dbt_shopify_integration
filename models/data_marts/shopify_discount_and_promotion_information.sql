{{ config(database = 'redaspen') }}

{{ config(schema = 'orders')}}

-- Creates a Transient Table that within Snowflake that houses Shopify discount/promotion information
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
-- Grabs all price rule information
),
discount_code AS(SELECT id,
                    price_rule_id,
                    code,    
                    usage_count
                FROM {{ source('shopify_raw', 'DISCOUNT_CODE') }}
-- Grabs all discount code information
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
    pr.value*100 AS value_cents
FROM price_rule pr JOIN discount_code dc ON pr.id = dc.price_rule_id
-- Organizes table into it's final format



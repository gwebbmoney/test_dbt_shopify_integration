{{ config(database = 'redaspen') }}

{{ config(schema = 'transaction_metrics')}}

-- Creates a Transient Table within Snowflake that houses both Infotrax and Shopify hostcredit information
WITH infotrax_hostcredits AS(
        SELECT *
        FROM {{ ref("int_infotrax__hostcredits") }}
-- Grabs all infotrax hostcredit information
),
shopify_pop_ups AS(
        SELECT dpo.order_id,
            dpo.total_discount_amount_cents AS hostcredit_amount_cents,
            dpo.created_at,
            o.brand_ambassador_id,
            dpo.distributor_status
        FROM {{ ref("shopify_discount_and_promotion_orders") }} dpo JOIN {{ ref("shopify_orders") }} o ON dpo.order_id = o.order_id
        WHERE order_discount_code LIKE '%POP%'
            OR order_discount_code LIKE '%TS%'
-- Grabs all Shopify pop up codes/redemptions
)
SELECT order_id,
    hostcredit_amount_cents,
    created_at,
    brand_ambassador_id,
    distributor_status
FROM shopify_pop_ups
UNION
SELECT order_id,
    hostcredit_amount_cents,
    created_at,
    brand_ambassador_id,
    distributor_status
FROM infotrax_hostcredits
-- Combines infotrax and shopify hostcredits
-- Organizes table into it's final format

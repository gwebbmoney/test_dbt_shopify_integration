{{ config(database = 'redaspen') }}

{{ config(schema = 'transaction_metrics')}}

-- Creates Transient Table within Snowflake that houses both Infotrax and Shopify refunds
WITH data_union AS({{dbt_utils.union_relations(
    relations = [ref('stg_shopify__refunds'), ref('stg_infotrax__refunds')]
)}}
)
SELECT ORDER_ID,
    REFUND_ID,
    REFUNDED_AT,
    PROCESSED_AT,
    BONUS_PERIOD,
    REFUND_AMOUNT_CENTS,
    NOTE,
    RESTOCK,
    (CASE
        WHEN _DBT_SOURCE_RELATION = 'FIVETRAN_SHOPIFY_RAW_DATA.dbt_shopify_transformations.stg_infotrax__refunds' THEN 'Infotrax'
        ELSE 'Shopify'
    END) AS SOURCE
FROM data_union
-- Organizes table for final format
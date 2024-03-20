{{ config(database = 'redaspen') }}

{{ config(schema = 'orders')}}

WITH data_union AS({{dbt_utils.union_relations(
    relations = [ref("int_shopify__order_line_refund"), ref("int_infotrax__order_line_refund")]
)}}
)
SELECT ORDER_ID,
    ORDER_LINE_ID,
    REFUND_ID,
    SKU,
    PRODUCT_ID,
    PRODUCT_NAME,
    RESTOCK_TYPE,
    REFUND_PRICE_CENTS,
    REFUND_QUANTITY,
    REFUND_TAX_CENTS,
    PRE_TAX_REFUND_CENTS,
    BUNDLE_PROPERTIES,
    (CASE
        WHEN _DBT_SOURCE_RELATION = 'FIVETRAN_SHOPIFY_RAW_DATA.dbt_shopify_transformations.int_infotrax__order_line_refund' THEN 'Infotrax'
        ELSE 'Shopify'
    END) AS source
FROM data_union 




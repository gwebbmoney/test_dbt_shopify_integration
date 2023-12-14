{{ config(database = 'redaspen_v2') }}

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
    REFUND_ORDER_LINE,
    QUANTITY_RETURNED,
    LINE_ITEM_PRICE_CENTS,
    (CASE
        WHEN _DBT_SOURCE_RELATION = 'FIVETRAN_SHOPIFY_RAW_DATA.transformed_shopify_api.int_infotrax__order_line_refund' THEN 'Infotrax'
        ELSE 'Shopify'
    END) AS source
FROM data_union 




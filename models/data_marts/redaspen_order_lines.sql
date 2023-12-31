{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'orders')}}

WITH data_union AS({{dbt_utils.union_relations(
    relations = [ref('int_shopify__order_lines'), ref('int_infotrax__order_lines')]
)}})
SELECT ORDER_LINE_ID,
ORDER_ID,
SHOPIFY_PRODUCT_ID,
EMMA_PRODUCT_ID,
PRODUCT_VARIANT_ID,
PRODUCT_NAME,
PRODUCT_VARIANT_NAME,
SKU,
BUNDLE_PROPERTIES,
ORDER_LINE,
SKUABLE_TYPE,
BUNDLE_TYPE,
PRODUCT_TAG,
PRICE_CENTS,
QUANTITY_ORDERED,
FULFILLABLE_QUANTITY,
LINE_ITEM_PRICE_CENTS,
TOTAL_DISCOUNT_CENTS,
PRE_TAX_PRICE_CENTS,
GIFT_CARD,
(CASE 
    WHEN _DBT_SOURCE_RELATION = 'FIVETRAN_SHOPIFY_RAW_DATA.dbt_shopify_transformations.int_infotrax__order_lines' THEN 'Infotrax'
    ELSE 'Shopify'
END) AS SOURCE
FROM data_union

{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'orders')}}

-- NOTE: DO NOT USE. DATABASE NOT IN SNOWFLAKE ANYMORE
WITH data_union AS({{dbt_utils.union_relations(
    relations = [ref('int_shopify__order_lines'), ref('int_infotrax__order_lines')]
)}})
SELECT du.ORDER_LINE_ID,
du.ORDER_ID,
o.ORDER_NUMBER,
du.SHOPIFY_PRODUCT_ID,
du.EMMA_PRODUCT_ID,
du.PRODUCT_VARIANT_ID,
du.PRODUCT_NAME,
du.PRODUCT_VARIANT_NAME,
du.SKU,
du.BUNDLE_PROPERTIES,
du.ORDER_LINE,
du.SKUABLE_TYPE,
du.BUNDLE_TYPE,
du.PRODUCT_TAG,
du.PRICE_CENTS,
du.QUANTITY_ORDERED,
du.FULFILLABLE_QUANTITY,
du.LINE_ITEM_PRICE_CENTS,
du.BUNDLE_DISCOUNT_CENTS,
du.SUBTOTAL_PRICE_CENTS,
du.LINE_ITEM_ORDER_DISCOUNT_CENTS,
du.PRE_TAX_PRICE_CENTS,
du.GIFT_CARD,
(CASE 
    WHEN du._DBT_SOURCE_RELATION = 'FIVETRAN_SHOPIFY_RAW_DATA.dbt_shopify_transformations.int_infotrax__order_lines' THEN 'Infotrax'
    ELSE 'Shopify'
END) AS SOURCE
FROM data_union du JOIN {{ ref('redaspen_orders') }} o ON du.order_id = o.order_id

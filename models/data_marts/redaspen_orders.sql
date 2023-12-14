{{ config(schema = 'practice') }}

WITH data_union AS({{dbt_utils.union_relations(
    relations = [ref('int_shopify__orders'), ref('int_infotrax__orders')]
)}})
SELECT ORDER_ID,
ORDER_NUMBER,
SUBTOTAL_AMOUNT_CENTS,
SALES_TAX_AMOUNT_CENTS,
SHIPPING_AMOUNT_CENTS,
SHIPPING_TAX_AMOUNT_CENTS,
TOTAL_DISCOUNT_AMOUNT_CENTS,
ORDER_INVOICE_AMOUNT_CENTS,
BONUS_PERIOD,
CREATED_AT,
PROCESSED_AT,
CANCELLED_AT,
UPDATED_AT,
CANCEL_REASON,
FINANCIAL_STATUS,
FULFILLMENT_STATUS,
SUBTOTAL_REFUND_CENTS,
SALES_TAX_REFUND_CENTS,
SHIPPING_REFUND_CENTS,
SHIPPING_TAX_REFUND_CENTS,
ORDER_ADJUSTMENT_AMOUNT_CENTS,
ORDER_ADJUSTMENT_TAX_AMOUNT_CENTS,
ORDER_REFUND_AMOUNT_CENTS,
TOTAL_ORDER_AMOUNT_CENTS,
SHIPPING_ADDRESS_FIRST_NAME,
SHIPPING_ADDRESS_LAST_NAME,
SHIPPING_ADDRESS_NAME,
SHIPPING_ADDRESS_ONE,
SHIPPING_ADDRESS_TWO,
SHIPPING_ADDRESS_CITY,
SHIPPING_ADDRESS_STATE,
SHIPPING_ADDRESS_ZIP,
SHIPPING_ADDRESS_LONGITUDE,
SHIPPING_ADDRESS_LATITUDE,
BILLING_ADDRESS_FIRST_NAME,
BILLING_ADDRESS_LAST_NAME,
BILLING_ADDRESS_NAME,
BILLING_ADDRESS_ONE,
BILLING_ADDRESS_TWO,
BILLING_ADDRESS_CITY,
BILLING_ADDRESS_STATE,
BILLING_ADDRESS_ZIP,
BILLING_ADDRESS_LONGITUDE,
BILLING_ADDRESS_LATITUDE,
CUSTOMER_ID,
USER_ID,
CHECKOUT_ID,
CHECKOUT_TOKEN,
REFERRING_SITE,
APP_ID,
ORDER_TAG_TYPE,
DISTRIBUTOR_STATUS,
BUYER_ACCEPTS_MARKETING,
SPONSOR_ID,
BRANDAMBASSADORID,
PARTYID,
HOSTID,
DATEOFBIRTH,
NOTE_ATTRIBUTES,
SPHERE_ORDER_NUMBER_REFERENCE,
INFOTRAX_ORDER_NUMBER_REFERENCE,
(CASE
    WHEN _DBT_SOURCE_RELATION = 'FIVETRAN_SHOPIFY_RAW_DATA.transformed_shopify_api.int_infotrax__orders' THEN 'Infotrax'
    ELSE 'Shopify'
END) AS SOURCE,
_FIVETRAN_DELETED,
_FIVETRAN_SYNCED,
DISCOUNT_REFUND_CENTS
FROM data_union






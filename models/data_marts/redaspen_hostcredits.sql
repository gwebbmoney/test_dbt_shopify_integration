--{{ config(database = 'redaspen') }}
--Will be used to transfer this table to the REDASPEN Schema

--{{config(schema = 'transaction_metrics') }}

{{dbt_utils.union_relations(
    relations = [ref('int_shopify__giftcards'), ref('int_infotrax__hostcredits')]
)}}
{{ config(database = 'redaspen') }}

{{ config(schema = 'transaction_metrics')}}

{{dbt_utils.union_relations(
    relations = [ref('int_shopify__giftcards'), ref('int_infotrax__hostcredits')]
)}}
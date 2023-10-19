{{dbt_utils.union_relations(
    relations = [ref('stg_shopify__refunds', ref('stg_infotrax__refunds')]
)}}
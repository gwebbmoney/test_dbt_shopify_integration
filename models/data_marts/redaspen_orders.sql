{{dbt_utils.union_relations(
    relations = [ref('int_shopify__orders'), ref('int_infotrax__orders')]
)}}







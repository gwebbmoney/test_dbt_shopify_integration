{{dbt_utils.union_relations(
    relations = [ref('int_shopify__order_lines'), ref('int_infotrax__order_lines')]
)}}
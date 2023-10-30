{{dbt_utils.union_relations(
    relations = [ref("int_shopify__order_line_refund"), ref("int_infotrax__order_line_refund")]
)}}
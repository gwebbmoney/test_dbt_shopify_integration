
-- NOTE: DO NOT USE. DATABASE NOT IN SNOWFLAKE ANYMORE
{{dbt_utils.union_relations(
    relations = [ref('int_shopify__giftcards'), ref('int_infotrax__hostcredits')]
)}}
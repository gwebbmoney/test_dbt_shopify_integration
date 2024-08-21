-- Creates a Transient Table in Snowflake that houses both Infotrax and Shopify bundle information
select
    /* Primary key */
    id,

    /* Foreign keys and IDs */
    emma_id,
    shopify_bundle_id,
    shopify_bundle_variant_id,

    /* Status and properties */
    bundle_title,
    shopify_bundle_title,
    shopify_bundle_variant_title,
    bundle_category_type,
    sku,
    bundle_type,
    bundle_source

from {{ ref('int_shopify__bundle_variants') }}

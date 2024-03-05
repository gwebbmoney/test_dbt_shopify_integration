{{ config(database="redaspen") }}

{{ config(schema="bundles") }}

with
    bundle_tag as (select * from {{ source("shopify_raw", "PRODUCT_TAG") }}),
    bundle_type_tag as (
        select product_id, value
        from bundle_tag
        where value = 'Bundle_Fixed' or value = 'Bundle_Custom' or value = 'LoyaltyBox'
    ),
    bundles as (
        select p.*
        from {{ ref("int_shopify__products") }} p
        join bundle_tag bt on p.product_id = bt.product_id
        where bt.value = 'Bundle' or bt.value = 'LoyaltyBox'
    ),
    bundle_variants as (
        select
            bv.id as bundle_variant_id,
            bv.title as bundle_variant_title,
            b.product_id as bundle_id,
            b.emma_product_id as emma_bundle_id,
            b.product_title as bundle_title,
            btt.value as bundle_type,
            bv.sku
        from {{ source("shopify_raw", "PRODUCT_VARIANT") }} bv
        join bundles b on bv.product_id = b.product_id
        left join bundle_type_tag btt on b.product_id = btt.product_id
    )
select
    cast(coalesce(b.product_id, bv.emma_bundle_id) as number) as emma_id,
    bv.bundle_id as shopify_bundle_id,
    coalesce(b.product_title, bv.bundle_title) as bundle_title,
    bv.bundle_title as shopify_bundle_title,
    bv.bundle_variant_id as shopify_bundle_variant_id,
    bv.bundle_variant_title as shopify_bundle_variant_title,
    coalesce(b.sku, bv.sku) as sku,
    coalesce(bundle_type, skuable_type) as bundle_type,
    (
        case when b.product_id = emma_id then 'Infotrax' else 'Shopify' end
    ) as bundle_source
from bundle_variants bv
full outer join {{ ref("int_infotrax__products") }} b on bv.sku = b.sku
where
    bundle_type in ('Bundle_Custom', 'Bundle_Fixed', 'LoyaltyBox')
    or skuable_type = 'Bundle'

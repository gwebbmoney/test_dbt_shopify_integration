-- Creates a Transient Table in Snowflake that houses both Infotrax and Shopify bundle information
with
    bundle_tag as (
        -- Grabs all product tags
        select
            product_id,
            product_tag
        from {{ ref('source_shopify__product_tag') }}
    ),
    shopify_products as (
        select *
        from {{ ref('int_shopify__products') }}
    ),
    infotrax_products as (
        select *
        from {{ ref('int_infotrax__products') }}
    ),
    product_variant as (
        select
            id as bundle_variant_id,
            product_id,
            bundle_variant_title,
            sku
        from {{ ref('source_shopify__product_variant') }}
    ),
   
    bundle_type_tag as (
        -- Grabs all product tags where a bundle is included 
        select *    
        from bundle_tag
        where trim(lower(product_tag)) in ('bundle_fixed', 'bundle_custom', 'loyaltybox')
    ),

    bundles as (
        -- Grabs all bundles that have a bundle tag
        select
            products.product_id as bundle_id,
            products.emma_product_id::number as emma_bundle_id,
            products.product_title as bundle_title,
            products.product_type as bundle_category_type
        from shopify_products as products
        inner join bundle_tag 
            on products.product_id = bundle_tag.product_id
        where trim(lower(bundle_tag.product_tag)) in ('bundle','loyaltybox')
    ),

    bundle_variants as (
        -- Grabs all information relating to a bundle
        select
            product_variant.*,
            bundles.*,
            bundle_type_tag.product_tag as bundle_type
        from product_variant
        inner join bundles 
            on product_variant.product_id = bundles.bundle_id
        left join bundle_type_tag 
            on bundles.bundle_id = bundle_type_tag.product_id
    )

-- Creates a table that only contains bundle information
-- Organizes table into it's final format
select
    coalesce(infotrax_products.product_id, bundle_variants.emma_bundle_id) as emma_id,
    bundle_variants.bundle_id as shopify_bundle_id,
    {{ dbt_utils.generate_surrogate_key(['emma_id', 'shopify_bundle_id']) }} as id,
    coalesce(infotrax_products.product_title, bundle_variants.bundle_title) as bundle_title,
    bundle_variants.bundle_title as shopify_bundle_title,
    bundle_variants.bundle_variant_id as shopify_bundle_variant_id,
    bundle_variants.bundle_variant_title as shopify_bundle_variant_title,
    bundle_variants.bundle_category_type,
    coalesce(infotrax_products.sku, bundle_variants.sku) as sku,
    coalesce(bundle_variants.bundle_type, infotrax_products.skuable_type) as bundle_type,
    case 
        when infotrax_products.product_id = emma_id then 'Infotrax' 
        else 'Shopify' 
    end as bundle_source
from bundle_variants
full outer join infotrax_products
    on bundle_variants.sku = infotrax_products.sku
where
    trim(lower(bundle_type)) in ('bundle_custom', 'bundle_fixed', 'loyaltybox')
    or infotrax_products.skuable_type = 'Bundle'

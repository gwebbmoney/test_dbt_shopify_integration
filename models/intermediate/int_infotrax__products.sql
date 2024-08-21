-- Creates view of all products and bundles shown within EMMA
-- NOTE: EMMA is our in-house application that houses various company data. For this purpose, we grab product information from this resource and attach it to the order lines table
with
    normalized_products as (
        select *
        from {{ ref('source_normalized_snowflake__products') }}
    ),
    skus as (
        select *
        from {{ ref('source_emma__skus') }}
    ),
    categories as (
        select *
        from {{ ref('source_emma__categories') }}
    ),
    sub_categories as (
        select *
        from {{ ref('source_emma__sub_categories')}}
    ),
    bundles as (
        select *
        from {{ ref('source_emma__bundles')}}
    ),

    emma_products as (
        -- Grabs all products within EMMA
        select
            {{ dbt_utils.generate_surrogate_key([
                'normalized_products.id', 
                'skus.sku'
            ]) }} as id,
            normalized_products.id as product_id,
            skus.sku,
            normalized_products.product_title,
            categories.id as category_id,
            categories.product_type,
            sub_categories.id as sub_category_id,
            sub_categories.sub_category_name,
            normalized_products.seasonality,
            normalized_products.collection,
            normalized_products.style_id,
            normalized_products.style,
            normalized_products.length_id,
            normalized_products.length,
            normalized_products.shape_id,
            normalized_products.shape,
            normalized_products.design,
            normalized_products.volume_id,
            normalized_products.volume,
            normalized_products.status_id,
            normalized_products.status,
            normalized_products.finish,
            normalized_products.price,
            normalized_products.pv,
            normalized_products.is_component,
            null as value,
            skus.skuable_type
        from normalized_products
        left join skus
            on normalized_products.id = skus.skuable_id
        left join categories 
            on normalized_products.category_id = categories.id
        left join sub_categories
            on normalized_products.sub_category_id = sub_categories.id
        where trim(lower(skus.skuable_type)) = 'product'
    ),

    emma_bundles as (
    -- Grabs all bundles within EMMA
        select
            {{ dbt_utils.generate_surrogate_key([
                'bundles.id', 
                'skus.sku'
            ]) }} as id,
            bundles.id as bundle_id,
            skus.sku,
            bundles.bundle_title,
            null as category_id,
            null as product_type,
            null as sub_category_id,
            null as sub_category_name,
            null as seasonality,
            null as collection,
            null as style_id,
            null as style,
            null as length_id,
            null as length,
            null as shape_id,
            null as shape,
            null as design,
            null as volume_id,
            null as volume,
            bundles.status_id,
            null as status,
            null as finish,
            bundles.price,
            bundles.pv,
            null as component,
            bundles.value,
            skus.skuable_type
        from bundles
        left join skus on bundles.id = skus.skuable_id
        where trim(lower(skus.skuable_type)) = 'bundle'
    ),

    unioned as (
        select *
        from emma_products
        union all
        select *
        from emma_bundles
    )

select * from unioned

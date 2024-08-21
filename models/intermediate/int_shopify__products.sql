-- Creates a view that grabs all products that are within our Shopify system
-- NOTE: EMMA is our in-house application that houses various company data. For this purpose, we grab product information from this resource and attach it to the order lines table
with
    products as (
        -- Grabs Shopify products in the SHOPIFY.PRODUCT table
        select 
            id as product_id,
            product_title,
            product_type,
            status,
            created_at,
            updated_at,
            published_at
        from {{ ref('source_shopify__products') }}
    ),
    
    metafield as (
        select 
            owner_id,
            key,
            value
        from {{ ref('source_shopify__metafield') }}
        where trim(lower(owner_resource)) = 'product' 
    ),

    product_metafield_emma_id as (
        -- Grabs the EMMA product id for each product
        select *
        from metafield
        where key = 'emma_id'
    ),

    product_metafield_sub_category as (
        -- Grabs the EMMA sub category id for each product
        select *
        from metafield
        where key = 'product_sub_category'
    )

-- Organizes product information from Shopify
select
    /* Primary key */
    products.product_id,

    /* Foreign keys and IDs */
    product_emma_id.value as emma_product_id,

    /* Timestamps */
    products.created_at,
    products.updated_at,
    products.published_at,

    /* Status and properties */
    product_sub_category.value as sub_category_name,
    products.product_title,
    products.product_type,
    products.status

from products 
left join product_metafield_emma_id as product_emma_id 
    on products.product_id = product_emma_id.owner_id
left join product_metafield_sub_category as product_sub_category  
    on products.product_id = product_sub_category.owner_id

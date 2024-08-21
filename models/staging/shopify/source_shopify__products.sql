select
    /* Primary key */
    id,

    /* Timestamps */
    created_at,
    updated_at,
    published_at,
    _fivetran_synced as fivetran_synced_at,

    /* Status and properties */
    title AS product_title,
    product_type,
    status

from {{ source('shopify', 'product') }}
where _fivetran_deleted = false

select
    /* Primary key */
    id,

    /* Foreign keys and IDs */
    product_id,

    /* Timestamps */
    created_at,
    updated_at,
    _fivetran_synced as fivetran_synced_at,

    /* Status and properties */
    sku,
    title as bundle_variant_title

from {{ source('shopify', 'product_variant') }}

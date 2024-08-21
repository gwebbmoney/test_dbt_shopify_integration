select
    /* Primary key */
    id,

    /* Foreign keys and IDs */
    skuable_id,

    /* Timestamps */
    _fivetran_synced as fivetran_synced_at,

    /* Status and properties */
    name as sku,
    skuable_type

from {{ source('emma', 'skus') }}

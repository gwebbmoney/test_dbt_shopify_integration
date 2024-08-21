select
    /* Primary key */
    id,

    /* Foreign keys and IDs */
    owner_id,

    /* Timestamps */
    created_at,
    updated_at,
    _fivetran_synced as fivetran_synced_at,

    /* Status and properties */
    key,
    value,
    owner_resource

from {{ source('shopify', 'metafield') }}

select
    /* Primary key */
    id,

    /* Foreign keys and IDs */
    status_id,

    /* Timestamps */
    created_at,
    updated_at,
    go_live,

    /* Status and properties */
    name as bundle_title,
    price,
    pv,
    value

from {{ source('emma', 'bundles') }}

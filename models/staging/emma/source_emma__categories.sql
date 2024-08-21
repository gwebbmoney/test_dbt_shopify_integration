select
    /* Primary key */
    id,

    /* Timestamps */
    created_at,
    updated_at,

    /* Status and properties */
    name as product_type

from {{ source('emma', 'categories') }}

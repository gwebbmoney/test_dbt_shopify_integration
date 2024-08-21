select
    /* Primary key */
    id,

    /* Timestamps */
    created_at,
    updated_at,

    /* Status and properties */
    name as sub_category_name

from {{ source('emma', 'sub_categories') }}

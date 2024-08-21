select
    /* Primary key */
    id,

    /* Foreign keys and IDs */
    style_id,
    length_id,
    shape_id,
    volume_id,
    status_id,
    category_id,
    sub_category_id,

    /* Timestamps */
    updated_at,

    /* Flags */
    component as is_component,

    /* Status and properties */
    name as product_title,
    seasonality,
    collection,
    style,
    length,
    shape,
    design,
    volume,
    status,
    finish,
    price,
    pv

from {{ source('normalized_snowflake', 'products') }}

select
    /* Primary key */
    {{ dbt_utils.generate_surrogate_key(['product_id', 'index']) }} as id

    /* Foreign keys and IDs */
    , product_id
    , index

    /* Timestamps */
    , _fivetran_synced as fivetran_synced_at

    /* Status and properties */
    , value as product_tag

from {{ source('shopify', 'product_tag') }}

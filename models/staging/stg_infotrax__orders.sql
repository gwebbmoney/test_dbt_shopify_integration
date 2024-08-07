-- Creates order table from Infotrax
-- Contains all order data that is ingested from Infotrax
-- Emulates current structure seen in Snowflake (REDASPEN.ORDERS.ORDERS)
SELECT 
    order_number as infotrax_order_number,
    discount_amount * 100 as discount_amount_cents,
    sales_tax * 100 as sales_tax_cents,
    total_invoice * 100 as total_invoice_cents,
    price_1 * 100 as retail_amount_cents,
    price_2 * 100 as pv_qualifying_amount_cents,
    price_3 * 100 as taxable_amount_cents,
    price_4 * 100 as commissionable_volume_cents,
    freight_amount * 100 as freight_amount_cents,
    distributor_status,
    order_source,
    order_status,
    order_type,
    date_from_parts(left(bonus_period,4),right(bonus_period,2),1) as bonus_period,
    timestamp_NTZ_from_parts(entry_date,
        time_from_parts(left(lpad(entry_time,8,'0'),2), 
                        substr(lpad(entry_time,8,'0'),3,2), 
                        substr(lpad(entry_time,8,'0'),5,2)
                        )
        ) as entered_at,
    timestamp_NTZ_from_parts(post_date,
        time_from_parts(left(lpad(post_time,8,'0'),2), 
                        substr(lpad(post_time,8,'0'),3,2), 
                        substr(lpad(post_time,8,'0'),5,2)
                        )
        ) as posted_at,
    country_code,
    currency_code,
    distributor_rank,
    discount_percent,
    email_address,
    language,
    last_update_time AS updated_at,
    ship_to_addr_1,
    ship_to_addr_2,
    ship_to_city,
    ship_to_country,
    ship_to_name,
    ship_to_phone,
    ship_to_state,
    ship_to_zip, 
    distributor_id,
    original_order as infotrax_original_order,
    _fivetran_deleted
FROM {{ source('raw_infotrax', 'ORDERMASTER') }}
WHERE _fivetran_deleted = FALSE
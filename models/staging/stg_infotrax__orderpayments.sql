WITH source AS(
    SELECT * FROM {{ source('raw_infotrax', 'ORDERPAYMENTS') }}
)
SELECT id,
    order_number AS infotrax_order_number,
    billing_first_name,
    billing_last_name,
    credit_card_name,
    billing_addr1,
    billing_addr2,
    billing_city,
    billing_zip,
    billing_country_code,
    entry_date,
    currency_code,
    auth_date,
    last_update_time,
    _fivetran_deleted,
    _fivetran_synced
FROM source


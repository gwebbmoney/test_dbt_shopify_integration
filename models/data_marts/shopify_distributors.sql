{{ config(database = 'redaspen') }}

{{ config(schema = 'distributors')}}

WITH shopify_customers AS(
    SELECT * FROM {{ ref("int_shopify__customers") }}
),
redaspen_distributors AS(
    SELECT * FROM {{ ref("int_redaspen__distributors") }}
)
SELECT sc.customer_id AS shopify_customer_id,
    CAST(sc.brand_ambassador_id AS NUMBER) AS brand_ambassador_id,
    rd.email_address AS infotrax_email,
    sc.email AS shopify_email,
    sc.first_name,
    sc.last_name,
    rd.distributor_name AS brand_ambassador_name,
    CAST(COALESCE(sc.sponsor_id, rd.sponsor_id) AS NUMBER) AS sponsor_id,
    partner_site,
    COALESCE(sc.distributor_status, rd.distributor_status) AS distributor_status,
    site,
    sc.note,
    sc.phone,
    sc.verified_email,
    sc.email_marketing_consent_opt_in_level,
    sc.email_marketing_consent_state,
    sc.email_marketing_consent_consent_updated_at,
    sc.address_1,
    sc.address_2,
    sc.city,
    sc.state,
    sc.country_code,
    sc.zip AS zip_code,
    sc.longitude,
    sc.latitude,
    sc.created_at,
    sc.updated_at,
    sc.metafield
FROM shopify_customers sc LEFT JOIN redaspen_distributors rd ON sc.brand_ambassador_id = rd.distributor_id
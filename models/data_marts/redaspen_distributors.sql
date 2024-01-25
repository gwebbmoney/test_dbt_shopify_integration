--{{ config(database = 'redaspen') }}
--Will be used to transfer this table to the REDASPEN Schema

{{ config(database = 'redaspen_v2') }}

{{ config(schema = 'distributors')}}

WITH shopify_customers AS(
    SELECT * FROM {{ ref("int_shopify__customers") }}
),
redaspen_distributors AS(
    SELECT * FROM {{ ref("int_redaspen__distributors") }}
)
SELECT CAST(COALESCE(rd.distributor_id, sc.brand_ambassador_id) AS NUMBER) AS brand_ambassador_id,
    rd.email_address AS infotrax_email,
    sc.email AS shopify_email,
    sc.first_name,
    sc.last_name,
    rd.distributor_name AS brand_ambassador_name,
    CAST(COALESCE(sc.sponsor_id, rd.sponsor_id) AS NUMBER) AS sponsor_id,
    COALESCE(sc.distributor_status, rd.distributor_status) AS distributor_status,
    rd.distributor_status AS ds,
    sc.note,
    sc.verified_email,
    sc.email_marketing_consent_opt_in_level,
    sc.email_marketing_consent_state,
    sc.email_marketing_consent_consent_updated_at,
    rd.address_1,
    rd.city,
    rd.state,
    rd.zip_code,
    rd.address_2,
    rd.address_3,
    rd.address_4
FROM shopify_customers sc FULL OUTER JOIN redaspen_distributors rd ON sc.brand_ambassador_id = rd.distributor_id
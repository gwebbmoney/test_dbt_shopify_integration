WITH shopify_customers AS(
    SELECT * FROM {{ ref("int_shopify__customers") }}
),
redaspen_distributors AS(
    SELECT * FROM {{ ref("int_redaspen__distributors") }}
)
SELECT sc.*,
    rd.address_1,
    rd.city,
    rd.state,
    rd.zip_code,
    rd.address_2,
    rd.address_3,
    rd.address_4
FROM shopify_customers sc LEFT JOIN redaspen_distributors rd ON sc.brandambassadorid = rd.distributor_id
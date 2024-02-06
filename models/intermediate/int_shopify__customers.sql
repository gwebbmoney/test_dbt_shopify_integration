WITH metadata AS(SELECT owner_id,
            key,
            value,
            owner_resource
        FROM {{ source('shopify_raw', 'METAFIELD') }}
        WHERE OWNER_RESOURCE IN('customer', 'CUSTOMER')
            AND key IN('InfoTraxID', 'MentorID', 'distributor_status')
),
customer_data AS(SELECT DISTINCT(m.owner_id) AS customer_id,
    c.email,
    MAX(CASE
        WHEN m.key = 'InfoTraxID' THEN m.value ELSE NULL END
    ) AS distributor_id,
    MAX(CASE
        WHEN m.key = 'MentorID' THEN m.value ELSE NULL END
    ) AS sponsor_id,
    MAX(CASE
        WHEN m.key = 'distributor_status' THEN m.value ELSE NULL END
    ) AS distributor_status
FROM metadata m LEFT JOIN {{ source('shopify_raw', 'CUSTOMER') }} c ON c.id = m.owner_id
GROUP BY m.owner_id, c.email
ORDER BY m.owner_id
),
customer_tag AS(SELECT customer_id,
                    MAX(SELECT REGEXP_SUBSTR(value, 'partner_site:\s*([^"]*)', 1, 1, 'i', 1)) AS partner_site,
                    MAX(SELECT REGEXP_SUBSTR(value, 'site:\s*([^"]*)', 1, 1, 'i', 1)) AS site
                FROM {{source('shopify_raw', 'CUSTOMER_TAG')}}
                GROUP BY customer_id
),
customer_address AS(SELECT customer_id,
                    address_1,
                    address_2,
                    city,
                    province_code AS state,
                    country_code,
                    zip,
                    latitude,
                    longitude
                FROM {{ source('shopify_raw', 'CUSTOMER_ADDRESS') }}
                WHERE is_default = TRUE
)
SELECT c.id AS customer_id,
    c.email,
    c.first_name,
    c.last_name,
    cd.distributor_id AS brand_ambassador_id,
    ct.site,
    cd.sponsor_id AS sponsor_id,
    ct.partner_site,
    cd.distributor_status,
    c.note,
    c.verified_email,
    c.email_marketing_consent_opt_in_level,
    c.email_marketing_consent_state,
    c.email_marketing_consent_consent_updated_at,
    ca.address_1,
    ca.address_2,
    ca.city,
    ca.state,
    ca.country_code,
    ca.zip,
    ca.latitude,
    ca.longitude,
    c.created_at,
    c.updated_at,
    c.metafield
FROM {{ source('shopify_raw', 'CUSTOMER') }} c LEFT JOIN customer_data cd ON c.id = cd.customer_id
    LEFT JOIN customer_address ca ON c.id = ca.customer_id
    LEFT JOIN customer_tag ct ON c.id = ct.customer_id
WHERE c._fivetran_deleted = FALSE

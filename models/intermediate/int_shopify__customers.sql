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
customer_tag AS(SELECT *
            FROM {{ source('shopify_raw', 'CUSTOMER_TAG') }}
            WHERE value IN ('partner_user', 'Affiliate', 'former_partner')
)
SELECT c.id AS customer_id,
    c.email,
    c.first_name,
    c.last_name,
    (CASE
        WHEN ct.value = 'partner_user' THEN value
        WHEN ct.value = 'Affiliate' THEN value
        WHEN ct.value = 'former_partner' THEN value
    END) as shopify_distributor_status,
    cd.distributor_id,
    cd.sponsor_id,
    cd.distributor_status,
    c.note,
    c.verified_email,
    c.email_marketing_consent_opt_in_level,
    c.email_marketing_consent_state,
    c.email_marketing_consent_consent_updated_at,
    c.created_at,
    c.updated_at,
    c.metafield
FROM {{ source('shopify_raw', 'CUSTOMER') }} c LEFT JOIN customer_data cd ON c.id = cd.customer_id
    LEFT JOIN customer_tag ct ON c.id = ct.customer_id
WHERE c._fivetran_deleted = FALSE
ORDER BY c.id;
SELECT *
FROM {{ source('raw_infotrax', 'DISTRIBUTORS') }}
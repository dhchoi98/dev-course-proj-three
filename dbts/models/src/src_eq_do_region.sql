{{ config(materialized='view') }}

WITH src_eq_do_region AS (
    SELECT
        *
    FROM {{ source('raw', 'eq_world') }}
    WHERE nkdiv = 'N' -- 북한 제외
    AND cntdiv = 'Y' AND MSGCODE = '지진정보'
)
-- 국내 지진 정보만 view로 구성
SELECT
    *
FROM src_eq_do_region
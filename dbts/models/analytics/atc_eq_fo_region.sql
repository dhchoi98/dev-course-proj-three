{{ config(
    materialized='table'
) }}
WITH regionBy AS(
    SELECT
        ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(eqpt, ' '), 0, 2), ' ') AS region_by,
        *
    FROM raw_data.raw_eq_world
    WHERE nkdiv = 'N' -- 북한 제외
    AND cntdiv = 'N' AND MSGCODE = '국외지진정보'
), cntBy AS(
    SELECT
        region_by,
        COUNT(*) AS total_count
    FROM regionBy
    GROUP BY region_by
), magmlBy AS(
    SELECT
        region_by,
        max(magMl) AS max_magml
    FROM regionBy
    GROUP BY region_by
)

SELECT
    c.region_by   AS region,
    c.total_count AS eq_count,
    m.max_magml
FROM cntBy AS c
JOIN magmlBy AS m ON c.region_by = m.region_by
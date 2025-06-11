{{ config(
    materialized='table'
) }}
WITH regionBy AS(
    SELECT
        *,
        ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(eqpt, ' '), 0, 2), ' ') AS region_by
    FROM {{ ref('src_eq_do_region') }}
), rankBy AS(
    SELECT
        region_by,
        eqlt,
        eqln
    FROM (
        SELECT
            region_by,
            eqlt,
            eqln,
            ROW_NUMBER() OVER(PARTITION BY region_by ORDER BY eqDate DESC) AS ranking
        FROM regionBy
    )
    WHERE ranking = 1
), cntBy AS(
    SELECT
        region_by,
        COUNT(*) AS total_count
    FROM regionBy
    GROUP BY region_by
)
SELECT
    r.region_by     AS region,
    c.total_count   AS eq_count,
    r.eqlt,
    r.eqln
FROM rankBy AS r
JOIN cntBy AS c ON r.region_by = c.region_by
ORDER BY 2 DESC

/*
  Query 03 — Star Rating Distribution
  =====================================
  Business Question:
    How are CMS star ratings distributed across the hospital population?
    Understanding the shape of this distribution tells policy makers whether
    quality is improving (right-skew) or declining (left-skew) and helps
    benchmark any individual hospital's relative position.

  Metrics:
    - Count and % of hospitals at each star level (1-5)
    - Cumulative count and running percentage (for distribution curves)
    - Average rating broken out by hospital type
    - Rating distribution heatmap data: state × rating bucket counts

  Technique:
    - GENERATE_SERIES ensures all rating levels (1-5) appear even if empty
    - Window functions compute cumulative totals
    - FILTER aggregates avoid multiple table scans
*/

-- ── Part 1: Overall rating distribution ──────────────────────────────────────
WITH all_ratings AS (
    -- Use generate_series to guarantee rows for all 5 star levels
    SELECT gs.rating_level
    FROM GENERATE_SERIES(1, 5) AS gs(rating_level)
),

rating_counts AS (
    SELECT
        overall_rating              AS rating_level,
        COUNT(*)                    AS hospital_count
    FROM hospitals
    WHERE overall_rating IS NOT NULL
    GROUP BY overall_rating
),

distribution AS (
    SELECT
        ar.rating_level,
        COALESCE(rc.hospital_count, 0)                              AS hospital_count,
        SUM(COALESCE(rc.hospital_count, 0)) OVER ()                 AS total_rated,

        -- Percentage of total rated hospitals
        ROUND(
            100.0 * COALESCE(rc.hospital_count, 0)
                  / NULLIF(SUM(COALESCE(rc.hospital_count, 0)) OVER (), 0),
            2
        )                                                           AS pct_of_rated,

        -- Cumulative count (low → high)
        SUM(COALESCE(rc.hospital_count, 0))
            OVER (ORDER BY ar.rating_level ROWS UNBOUNDED PRECEDING) AS cumulative_count,

        -- Cumulative percentage
        ROUND(
            100.0 * SUM(COALESCE(rc.hospital_count, 0))
                        OVER (ORDER BY ar.rating_level ROWS UNBOUNDED PRECEDING)
                  / NULLIF(SUM(COALESCE(rc.hospital_count, 0)) OVER (), 0),
            2
        )                                                           AS cumulative_pct

    FROM all_ratings ar
    LEFT JOIN rating_counts rc ON ar.rating_level = rc.rating_level
),

unrated AS (
    SELECT COUNT(*) AS unrated_count
    FROM hospitals
    WHERE overall_rating IS NULL
)

SELECT
    d.rating_level,
    CASE d.rating_level
        WHEN 1 THEN '★☆☆☆☆  (1 Star)'
        WHEN 2 THEN '★★☆☆☆  (2 Stars)'
        WHEN 3 THEN '★★★☆☆  (3 Stars)'
        WHEN 4 THEN '★★★★☆  (4 Stars)'
        WHEN 5 THEN '★★★★★  (5 Stars)'
    END                                                             AS rating_label,
    d.hospital_count,
    d.total_rated,
    u.unrated_count,
    d.pct_of_rated,
    d.cumulative_count,
    d.cumulative_pct
FROM distribution d
CROSS JOIN unrated u
ORDER BY d.rating_level;


-- ── Part 2: Rating distribution by hospital type ──────────────────────────────
SELECT
    hospital_type,
    COUNT(*)                                                        AS total,
    COUNT(*) FILTER (WHERE overall_rating IS NOT NULL)              AS rated,
    ROUND(AVG(overall_rating), 2)                                   AS avg_rating,
    COUNT(*) FILTER (WHERE overall_rating = 1)                      AS stars_1,
    COUNT(*) FILTER (WHERE overall_rating = 2)                      AS stars_2,
    COUNT(*) FILTER (WHERE overall_rating = 3)                      AS stars_3,
    COUNT(*) FILTER (WHERE overall_rating = 4)                      AS stars_4,
    COUNT(*) FILTER (WHERE overall_rating = 5)                      AS stars_5,
    -- Modal rating for each type
    MODE() WITHIN GROUP (ORDER BY overall_rating)                   AS modal_rating
FROM hospitals
GROUP BY hospital_type
ORDER BY avg_rating DESC NULLS LAST;

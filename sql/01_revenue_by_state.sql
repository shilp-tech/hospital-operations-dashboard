/*
  Query 01 — Hospital Distribution & Performance by State
  ========================================================
  Business Question:
    Which states have the highest concentration of hospitals, and how does
    average CMS star-rating vary across states?  This drives resource allocation
    decisions and helps identify states where overall quality may need attention.

  Metrics:
    - Total hospitals per state
    - Count of rated vs. unrated hospitals
    - Average, min, and max overall rating
    - Percentage of hospitals rated 4 or 5 stars ("high performers")
    - Count of hospitals with emergency services

  Output is ordered by hospital count DESC so the most hospital-dense
  states appear first — useful as a ranked report or bar-chart data feed.
*/

WITH state_base AS (
    SELECT
        state,
        COUNT(*)                                                    AS total_hospitals,

        -- Separate rated vs unrated (CMS leaves ~20% unrated)
        COUNT(*) FILTER (WHERE overall_rating IS NOT NULL)          AS rated_hospitals,
        COUNT(*) FILTER (WHERE overall_rating IS NULL)              AS unrated_hospitals,

        -- Core rating stats (only over rated hospitals)
        ROUND(AVG(overall_rating), 2)                               AS avg_rating,
        MIN(overall_rating)                                         AS min_rating,
        MAX(overall_rating)                                         AS max_rating,

        -- High performers: 4 or 5 stars
        COUNT(*) FILTER (WHERE overall_rating >= 4)                 AS high_performer_count,

        -- Emergency capacity
        COUNT(*) FILTER (WHERE emergency_services = TRUE)           AS hospitals_with_er

    FROM hospitals
    GROUP BY state
),

enriched AS (
    SELECT
        *,
        -- High-performer rate among rated hospitals
        CASE
            WHEN rated_hospitals > 0
            THEN ROUND(100.0 * high_performer_count / rated_hospitals, 1)
            ELSE NULL
        END AS pct_high_performers,

        -- Share of national hospital count
        ROUND(
            100.0 * total_hospitals / SUM(total_hospitals) OVER (), 2
        ) AS pct_of_national_total,

        -- Rank by hospital count
        RANK() OVER (ORDER BY total_hospitals DESC)                 AS rank_by_count,
        RANK() OVER (ORDER BY AVG(overall_rating) DESC NULLS LAST) AS rank_by_avg_rating

    FROM state_base
)

SELECT
    rank_by_count,
    state,
    total_hospitals,
    rated_hospitals,
    unrated_hospitals,
    avg_rating,
    min_rating,
    max_rating,
    high_performer_count,
    pct_high_performers,
    hospitals_with_er,
    pct_of_national_total,
    rank_by_avg_rating
FROM enriched
ORDER BY total_hospitals DESC, state;

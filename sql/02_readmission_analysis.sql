/*
  Query 02 — Readmission Analysis
  =================================
  Business Question:
    Which hospitals have readmission rates worse than the national average,
    and is there a correlation between readmission performance and overall
    star rating?  Identifying outliers supports quality improvement targeting.

  Approach:
    - Join hospitals with timely_care to get numeric readmission scores
      (measure READM_30_HOSP_WIDE = hospital-wide 30-day unplanned readmission)
    - Derive a numeric "readmission flag" from the national comparison field
    - Compute a national average from the measure scores
    - Flag hospitals above (worse than) the national average
    - Segment results into performance tiers

  Notes:
    - Lower readmission rate = better performance
    - "Worse than national average" in CMS categorical field = problem flag
    - Hospitals with no timely_care data still appear via LEFT JOIN
*/

WITH readmission_scores AS (
    -- Pull the hospital-wide 30-day readmission measure
    SELECT
        provider_id,
        AVG(score) AS readmission_rate   -- some hospitals have multiple periods
    FROM timely_care
    WHERE measure_id = 'READM_30_HOSP_WIDE'
      AND score IS NOT NULL
    GROUP BY provider_id
),

national_avg AS (
    SELECT AVG(score) AS national_readmission_avg
    FROM timely_care
    WHERE measure_id = 'READM_30_HOSP_WIDE'
      AND score IS NOT NULL
),

hospital_base AS (
    SELECT
        h.provider_id,
        h.name,
        h.state,
        h.city,
        h.hospital_type,
        h.ownership,
        h.overall_rating,

        -- Categorical readmission comparison from CMS
        h.readmission_national_comparison,

        -- Numeric readmission score (NULL if no timely_care record)
        rs.readmission_rate,

        -- Convert categorical CMS comparison to a direction flag
        CASE h.readmission_national_comparison
            WHEN 'Above the national average' THEN 'Better'      -- confusing CMS naming: "above" = better
            WHEN 'Same as the national average' THEN 'Same'
            WHEN 'Below the national average' THEN 'Worse'
            ELSE 'Not Rated'
        END AS readmission_direction

    FROM hospitals h
    LEFT JOIN readmission_scores rs USING (provider_id)
),

with_national AS (
    SELECT
        hb.*,
        na.national_readmission_avg,

        -- Flag hospitals with numeric score worse than national avg
        CASE
            WHEN hb.readmission_rate > na.national_readmission_avg THEN TRUE
            WHEN hb.readmission_rate IS NULL THEN NULL
            ELSE FALSE
        END AS above_national_avg,

        -- Deviation from national average (positive = worse)
        ROUND(hb.readmission_rate - na.national_readmission_avg, 2) AS deviation_from_avg

    FROM hospital_base hb
    CROSS JOIN national_avg na
),

tiered AS (
    SELECT
        *,
        -- Performance tier using numeric score where available,
        -- falling back to categorical field
        CASE
            WHEN readmission_direction = 'Worse'  THEN 'Needs Improvement'
            WHEN readmission_direction = 'Same'   THEN 'Average'
            WHEN readmission_direction = 'Better' THEN 'High Performing'
            ELSE 'Unrated'
        END AS performance_tier,

        RANK() OVER (
            ORDER BY readmission_rate DESC NULLS LAST
        ) AS rank_worst_readmission

    FROM with_national
)

SELECT
    rank_worst_readmission,
    provider_id,
    name,
    state,
    city,
    hospital_type,
    ownership,
    overall_rating,
    readmission_direction,
    readmission_rate,
    ROUND(national_readmission_avg, 2) AS national_avg,
    deviation_from_avg,
    above_national_avg,
    performance_tier
FROM tiered
ORDER BY
    -- Sort worst performers first, then unrated, then best
    CASE performance_tier
        WHEN 'Needs Improvement' THEN 1
        WHEN 'Unrated'           THEN 2
        WHEN 'Average'           THEN 3
        WHEN 'High Performing'   THEN 4
    END,
    readmission_rate DESC NULLS LAST;

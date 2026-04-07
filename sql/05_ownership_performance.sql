/*
  Query 05 — Ownership Type Performance Comparison
  ==================================================
  Business Question:
    Do non-profit, government, or for-profit hospitals deliver better
    outcomes?  This is a perennial policy debate — this query provides
    the data foundation for that analysis using CMS metrics.

  Metrics:
    - Average overall rating by ownership type
    - Patient experience score (derived from categorical CMS field)
    - Distribution of national comparison outcomes across dimensions
    - High-performer rate (% rated 4+ stars)
    - Emergency services availability

  Technique:
    - GROUP BY ownership with HAVING to filter out tiny sample sizes
    - FILTER aggregates for conditional counts in one pass
    - Derived numeric proxy score for patient experience:
        Above national avg  = 3
        Same                = 2
        Below               = 1
        Not Available       = NULL
    - Window functions to rank ownership types by performance

  Interpretation note:
    CMS hospital types within the same ownership category vary widely;
    always stratify by hospital_type when drawing policy conclusions.
*/

WITH numeric_scores AS (
    SELECT
        provider_id,
        ownership,
        hospital_type,
        overall_rating,
        emergency_services,

        -- Derive numeric proxy scores from categorical CMS comparison fields
        -- Scale: 3 = above avg, 2 = same, 1 = below, NULL = not available

        CASE patient_experience_national_comparison
            WHEN 'Above the national average' THEN 3
            WHEN 'Same as the national average' THEN 2
            WHEN 'Below the national average'  THEN 1
        END AS patient_exp_score,

        CASE mortality_national_comparison
            WHEN 'Above the national average' THEN 3
            WHEN 'Same as the national average' THEN 2
            WHEN 'Below the national average'  THEN 1
        END AS mortality_score,

        CASE safety_national_comparison
            WHEN 'Above the national average' THEN 3
            WHEN 'Same as the national average' THEN 2
            WHEN 'Below the national average'  THEN 1
        END AS safety_score,

        CASE readmission_national_comparison
            -- Note: CMS naming is inverse — "above avg" = better (fewer readmissions)
            WHEN 'Above the national average' THEN 3
            WHEN 'Same as the national average' THEN 2
            WHEN 'Below the national average'  THEN 1
        END AS readmission_score,

        CASE effectiveness_national_comparison
            WHEN 'Above the national average' THEN 3
            WHEN 'Same as the national average' THEN 2
            WHEN 'Below the national average'  THEN 1
        END AS effectiveness_score

    FROM hospitals
    WHERE ownership IS NOT NULL
),

ownership_summary AS (
    SELECT
        ownership,

        COUNT(*)                                                    AS total_hospitals,
        COUNT(*) FILTER (WHERE overall_rating IS NOT NULL)          AS rated_hospitals,

        -- Star rating
        ROUND(AVG(overall_rating), 3)                               AS avg_rating,
        ROUND(STDDEV(overall_rating), 3)                            AS stddev_rating,

        -- High performers (4+ stars) among rated
        COUNT(*) FILTER (WHERE overall_rating >= 4)                 AS high_performer_count,
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE overall_rating >= 4)
                  / NULLIF(COUNT(*) FILTER (WHERE overall_rating IS NOT NULL), 0),
            1
        )                                                           AS pct_high_performers,

        -- Low performers (1-2 stars)
        COUNT(*) FILTER (WHERE overall_rating <= 2)                 AS low_performer_count,

        -- Derived quality dimension averages
        ROUND(AVG(patient_exp_score), 3)                            AS avg_patient_exp_score,
        ROUND(AVG(mortality_score), 3)                              AS avg_mortality_score,
        ROUND(AVG(safety_score), 3)                                 AS avg_safety_score,
        ROUND(AVG(readmission_score), 3)                            AS avg_readmission_score,
        ROUND(AVG(effectiveness_score), 3)                          AS avg_effectiveness_score,

        -- Emergency service availability rate
        ROUND(
            100.0 * COUNT(*) FILTER (WHERE emergency_services = TRUE)
                  / NULLIF(COUNT(*), 0),
            1
        )                                                           AS pct_with_er

    FROM numeric_scores
    GROUP BY ownership

    -- Exclude ownership categories with very small samples (< 5 hospitals)
    -- to avoid statistically meaningless averages
    HAVING COUNT(*) >= 5
),

ranked AS (
    SELECT
        *,
        RANK() OVER (ORDER BY avg_rating DESC NULLS LAST)           AS rank_by_rating,
        RANK() OVER (ORDER BY pct_high_performers DESC NULLS LAST)  AS rank_by_high_performers,

        -- Composite quality score: weighted average of all dimension scores
        ROUND(
            (
                COALESCE(avg_patient_exp_score, 2) * 0.25 +
                COALESCE(avg_mortality_score, 2)   * 0.25 +
                COALESCE(avg_safety_score, 2)      * 0.20 +
                COALESCE(avg_readmission_score, 2) * 0.20 +
                COALESCE(avg_effectiveness_score, 2) * 0.10
            ), 3
        )                                                           AS composite_quality_score
    FROM ownership_summary
)

SELECT
    rank_by_rating,
    ownership,
    total_hospitals,
    rated_hospitals,
    avg_rating,
    stddev_rating,
    high_performer_count,
    pct_high_performers,
    low_performer_count,
    avg_patient_exp_score,
    avg_mortality_score,
    avg_safety_score,
    avg_readmission_score,
    avg_effectiveness_score,
    composite_quality_score,
    pct_with_er,
    rank_by_high_performers
FROM ranked
ORDER BY rank_by_rating;

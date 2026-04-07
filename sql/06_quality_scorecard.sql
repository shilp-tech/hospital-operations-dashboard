/*
  Query 06 — Hospital Quality Scorecard (Composite Score)
  =========================================================
  Business Question:
    How do individual hospitals score across ALL quality dimensions combined?
    A single composite score enables executive dashboards, peer benchmarking,
    and identification of hospitals that excel in some areas while struggling
    in others — a pattern invisible from the overall star rating alone.

  Methodology:
    Each CMS national comparison dimension is converted to a 0–100 sub-score:
      Above national avg  → 100
      Same as national avg → 50
      Below national avg  →   0
      Not Available       → NULL (excluded from weighted average)

    Dimension weights reflect CMS's own star-rating methodology weighting:
      Mortality               25%
      Safety of care          22%
      Readmission             22%
      Patient experience      22%
      Effectiveness            9%  (lower weight: process measure, not outcome)
      Timeliness               0%  (excluded: high missing-data rate)
      Efficient use            0%  (excluded: high missing-data rate)

    Composite = weighted average of available sub-scores (ignores NULLs).
    Hospitals with fewer than 3 scored dimensions are flagged as "Insufficient Data."

  Outputs:
    - Sub-score per dimension
    - Composite weighted score (0–100)
    - Letter grade (A/B/C/D/F)
    - Percentile rank nationally
    - Performance tier
    - Compared to: national avg, state avg, peer-type avg
*/

WITH dimension_scores AS (
    SELECT
        provider_id,
        name,
        state,
        city,
        hospital_type,
        ownership,
        overall_rating,          -- CMS star rating (1-5), kept for comparison

        -- ── Dimension sub-scores (0-100 scale) ───────────────────────────
        CASE mortality_national_comparison
            WHEN 'Above the national average' THEN 100
            WHEN 'Same as the national average' THEN 50
            WHEN 'Below the national average'  THEN 0
        END                                                 AS mortality_sub,

        CASE safety_national_comparison
            WHEN 'Above the national average' THEN 100
            WHEN 'Same as the national average' THEN 50
            WHEN 'Below the national average'  THEN 0
        END                                                 AS safety_sub,

        CASE readmission_national_comparison
            WHEN 'Above the national average' THEN 100
            WHEN 'Same as the national average' THEN 50
            WHEN 'Below the national average'  THEN 0
        END                                                 AS readmission_sub,

        CASE patient_experience_national_comparison
            WHEN 'Above the national average' THEN 100
            WHEN 'Same as the national average' THEN 50
            WHEN 'Below the national average'  THEN 0
        END                                                 AS patient_exp_sub,

        CASE effectiveness_national_comparison
            WHEN 'Above the national average' THEN 100
            WHEN 'Same as the national average' THEN 50
            WHEN 'Below the national average'  THEN 0
        END                                                 AS effectiveness_sub

    FROM hospitals
),

scored AS (
    SELECT
        *,

        -- Count how many dimensions have a non-NULL score
        (
            (mortality_sub IS NOT NULL)::INT +
            (safety_sub IS NOT NULL)::INT +
            (readmission_sub IS NOT NULL)::INT +
            (patient_exp_sub IS NOT NULL)::INT +
            (effectiveness_sub IS NOT NULL)::INT
        )                                                   AS scored_dimensions,

        -- ── Composite weighted score ─────────────────────────────────────
        -- Weighted average ignoring NULLs; re-normalise weights accordingly
        CASE
            WHEN (
                COALESCE(mortality_sub, -1) = -1 AND
                COALESCE(safety_sub, -1) = -1 AND
                COALESCE(readmission_sub, -1) = -1 AND
                COALESCE(patient_exp_sub, -1) = -1 AND
                COALESCE(effectiveness_sub, -1) = -1
            ) THEN NULL
            ELSE ROUND(
                (
                    COALESCE(mortality_sub, 0)    * 0.25 +
                    COALESCE(safety_sub, 0)       * 0.22 +
                    COALESCE(readmission_sub, 0)  * 0.22 +
                    COALESCE(patient_exp_sub, 0)  * 0.22 +
                    COALESCE(effectiveness_sub, 0) * 0.09
                ) /
                -- Normalize by the sum of weights for available dimensions
                NULLIF(
                    (mortality_sub IS NOT NULL)::INT    * 0.25 +
                    (safety_sub IS NOT NULL)::INT       * 0.22 +
                    (readmission_sub IS NOT NULL)::INT  * 0.22 +
                    (patient_exp_sub IS NOT NULL)::INT  * 0.22 +
                    (effectiveness_sub IS NOT NULL)::INT * 0.09,
                    0
                ) * 100,
                1
            )
        END                                                 AS composite_score

    FROM dimension_scores
),

with_benchmarks AS (
    SELECT
        *,
        -- National percentile rank
        PERCENT_RANK() OVER (
            ORDER BY composite_score NULLS FIRST
        ) * 100                                             AS national_percentile,

        -- State peer rank
        RANK() OVER (
            PARTITION BY state
            ORDER BY composite_score DESC NULLS LAST
        )                                                   AS state_rank,

        -- Hospital-type peer average
        ROUND(AVG(composite_score) OVER (
            PARTITION BY hospital_type
        ), 1)                                               AS peer_type_avg_score,

        -- National average
        ROUND(AVG(composite_score) OVER (), 1)             AS national_avg_score,

        -- State average
        ROUND(AVG(composite_score) OVER (PARTITION BY state), 1) AS state_avg_score

    FROM scored
),

graded AS (
    SELECT
        *,
        -- Letter grade based on composite score
        CASE
            WHEN composite_score IS NULL OR scored_dimensions < 3
                                                THEN 'N/A  (Insufficient Data)'
            WHEN composite_score >= 80          THEN 'A'
            WHEN composite_score >= 65          THEN 'B'
            WHEN composite_score >= 50          THEN 'C'
            WHEN composite_score >= 35          THEN 'D'
            ELSE                                     'F'
        END                                                 AS letter_grade,

        -- Performance tier
        CASE
            WHEN composite_score IS NULL OR scored_dimensions < 3
                                                THEN 'Insufficient Data'
            WHEN composite_score >= 75          THEN 'Top Performer'
            WHEN composite_score >= 50          THEN 'Average'
            WHEN composite_score >= 25          THEN 'Below Average'
            ELSE                                     'Low Performer'
        END                                                 AS performance_tier,

        -- Deviation from national average
        ROUND(composite_score - ROUND(AVG(composite_score) OVER(), 1), 1) AS vs_national_avg,

        -- Deviation from peer-type average
        ROUND(
            composite_score - ROUND(AVG(composite_score) OVER (PARTITION BY hospital_type), 1),
            1
        )                                                   AS vs_peer_type_avg

    FROM with_benchmarks
)

SELECT
    state_rank,
    provider_id,
    name,
    state,
    city,
    hospital_type,
    ownership,
    overall_rating,

    -- Dimension sub-scores
    mortality_sub,
    safety_sub,
    readmission_sub,
    patient_exp_sub,
    effectiveness_sub,
    scored_dimensions,

    -- Composite & grade
    composite_score,
    letter_grade,
    performance_tier,
    ROUND(national_percentile, 1)   AS national_percentile,

    -- Benchmarks
    national_avg_score,
    state_avg_score,
    peer_type_avg_score,
    vs_national_avg,
    vs_peer_type_avg

FROM graded
ORDER BY composite_score DESC NULLS LAST, name;

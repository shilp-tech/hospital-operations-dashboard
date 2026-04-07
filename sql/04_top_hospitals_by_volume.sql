/*
  Query 04 — Top Hospitals by Performance (Ranked Within State)
  ==============================================================
  Business Question:
    Who are the top-performing hospitals in each state, and how do they
    compare to their in-state peers?  State-level ranking is critical for
    state health departments, payers building narrow networks, and patients
    choosing where to receive care.

  Technique:
    - ROW_NUMBER() for a unique rank per state (no ties)
    - RANK() for a true competitive rank (ties share a rank)
    - DENSE_RANK() to show gap-free ordinal position
    - NTILE(4) to assign performance quartiles within each state
    - LAG() to show the rating of the next-lower-ranked hospital
      (useful for understanding competitive gaps)
    - Returns TOP 3 per state using ROW_NUMBER filter

  Note on "volume": CMS public data does not publish discharge volumes for
  individual hospitals.  This query uses overall_rating as the primary
  ranking criterion — consistent with how CMS publicly benchmarks hospitals.
  When discharge data is available (e.g., from internal EDW), replace
  `overall_rating` with the volume metric in the ORDER BY clause.
*/

WITH ranked AS (
    SELECT
        provider_id,
        name,
        state,
        city,
        hospital_type,
        ownership,
        overall_rating,
        emergency_services,

        -- Count total rated hospitals in the same state (peer group size)
        COUNT(*) FILTER (WHERE overall_rating IS NOT NULL)
            OVER (PARTITION BY state)                               AS state_rated_count,

        -- Unique rank within state (highest rating = rank 1)
        ROW_NUMBER() OVER (
            PARTITION BY state
            ORDER BY overall_rating DESC NULLS LAST, name
        )                                                           AS row_num,

        -- True competitive rank (ties get the same rank)
        RANK() OVER (
            PARTITION BY state
            ORDER BY overall_rating DESC NULLS LAST
        )                                                           AS rank_in_state,

        -- Dense rank (no gaps after ties)
        DENSE_RANK() OVER (
            PARTITION BY state
            ORDER BY overall_rating DESC NULLS LAST
        )                                                           AS dense_rank_in_state,

        -- Performance quartile within state (1 = top 25%, 4 = bottom 25%)
        NTILE(4) OVER (
            PARTITION BY state
            ORDER BY overall_rating DESC NULLS LAST
        )                                                           AS state_quartile,

        -- Rating of the next hospital below this one in the same state
        LAG(overall_rating, 1) OVER (
            PARTITION BY state
            ORDER BY overall_rating DESC NULLS LAST, name
        )                                                           AS next_lower_rating,

        -- State average (for comparison)
        ROUND(AVG(overall_rating) OVER (PARTITION BY state), 2)    AS state_avg_rating,

        -- National average (for comparison)
        ROUND(AVG(overall_rating) OVER (), 2)                       AS national_avg_rating

    FROM hospitals
    WHERE overall_rating IS NOT NULL
),

top_per_state AS (
    SELECT
        *,
        -- How many stars above/below state average
        ROUND(overall_rating - state_avg_rating, 2)                 AS stars_vs_state_avg,
        -- How many stars above/below national average
        ROUND(overall_rating - national_avg_rating, 2)              AS stars_vs_national_avg
    FROM ranked
    WHERE row_num <= 3   -- TOP 3 per state
)

SELECT
    state,
    row_num                     AS state_rank,
    rank_in_state,
    dense_rank_in_state,
    state_quartile,
    provider_id,
    name,
    city,
    hospital_type,
    ownership,
    overall_rating,
    state_avg_rating,
    national_avg_rating,
    stars_vs_state_avg,
    stars_vs_national_avg,
    state_rated_count,
    emergency_services
FROM top_per_state
ORDER BY state, row_num;

-- Average number of themes per day
/*
SELECT
  DATE(datetime) as date, AVG(CARDINALITY(themes)) as avg_num_themes
FROM
  gdelt_raw
GROUP BY
  date
ORDER BY
  date
*/

-- Find average per-day tone for articles that contain specific low-level themes
-- that make up at least 10% of all low-level themes (uses Housing themes).
/*
WITH themes_ref AS (
  SELECT
    ARRAY[
      'ECON_HOUSING_PRICES',
      'WB_612_HOUSING_THEMES',
      'WB_817_LAND_AND_HOUSING',
      'WB_904_HOUSING_MARKETS',
      'WB_2186_SOCIAL_HOUSING',
      'WB_870_HOUSING_CONSTRUCTION',
      'WB_2187_RENTAL_HOUSING',
      'WB_1722_HOUSING_POLICY_AND_INSTITUTIONS',
      'WB_871_HOUSING_SUBSIDIES',
      'WB_1594_HOUSING_ALLOWANCE',
      'WB_2188_AFFORDABLE_HOUSING_SUPPLY',
      'WB_2184_HOUSING_FINANCE_FOR_THE_POOR',
      'WB_869_HOUSING_LAWS_AND_REGULATIONS'
    ]
  AS
    housing
)
SELECT
  date,
  AVG(tone) as avg_tone,
  COUNT(*) as num_relevant_articles
FROM
  (
    -- Calculate num_themes and num_relevant_themes
    SELECT
      DATE(datetime) as date,
      tone,
      CARDINALITY(themes) as num_themes,
      CARDINALITY(array_intersect(
        themes_ref.housing,
        themes
      )) as num_relevant_themes
    FROM
      gdelt_raw, themes_ref
  ) t1
WHERE
  --date > '2021-07-01' AND  -- FOR QUICKER TESTING
  num_themes > 0 AND 1.0 * num_relevant_themes / num_themes > 0.1
GROUP BY
  date
ORDER BY
  date
;
*/

-- Previous, but controlling for syndication and determining onshore/offshore
-- WORK IN PROGRESS*****************************
WITH themes_ref AS (
  SELECT
    ARRAY[
      'ECON_HOUSING_PRICES',
      'WB_612_HOUSING_THEMES',
      'WB_817_LAND_AND_HOUSING',
      'WB_904_HOUSING_MARKETS',
      'WB_2186_SOCIAL_HOUSING',
      'WB_870_HOUSING_CONSTRUCTION',
      'WB_2187_RENTAL_HOUSING',
      'WB_1722_HOUSING_POLICY_AND_INSTITUTIONS',
      'WB_871_HOUSING_SUBSIDIES',
      'WB_1594_HOUSING_ALLOWANCE',
      'WB_2188_AFFORDABLE_HOUSING_SUPPLY',
      'WB_2184_HOUSING_FINANCE_FOR_THE_POOR',
      'WB_869_HOUSING_LAWS_AND_REGULATIONS'
    ]
  AS
    housing
)
SELECT
  date, AVG(tone) as tone,
  COUNT(prop_relevant_themes > 0.1) as relevant_articles,
  SUM(num_articles) as num_articles
FROM
  (
  -- Calculate prop_relevant_themes
  SELECT
    *,
    CASE
      WHEN num_themes = 0 then 0.0
      ELSE 1.0 * num_relevant_themes / num_themes
    END as prop_relevant_themes
  FROM
    (
      -- Calculate num_themes and num_relevant_themes *for each unique article*
      SELECT
        DATE(datetime) as date, AVG(tone) as tone,
        --UNNEST(countries) as country, source_name,
        COUNT(gkg_id) as num_articles,
        AVG(CARDINALITY(themes)) as num_themes,
        AVG(CARDINALITY(array_intersect(
          themes_ref.housing,
          themes
        ))) as num_relevant_themes
      FROM
        gdelt_raw, themes_ref
      WHERE DATE(datetime) > '2021-07-26' -- REMOVE
      GROUP BY
        date,
        --country, source_name,
        pos, neg, wc  -- to control for syndication
    ) t1
  ) t2
WHERE
  --prop_relevant_themes > 0.1
  (num_themes > 0 AND 1.0 * num_relevant_themes / num_themes > 0.1)
GROUP BY
  date
ORDER BY
  date, tone
;

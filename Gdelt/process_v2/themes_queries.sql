-- TODO:
-- 1) How to loop through sets of low-level themes (housing, unemployment etc.)
--    to construct collection of high-level theme indicators???
-- 2) Could the low-level themes be passed in from external file via bash
--    script (or is this unnecessarily complicated)? The thinking would be that
--    these low-level themes are generated using Python and could be updated in
--    the future, and hardcoding in SQL script seems error-prone.

CREATE OR REPLACE FUNCTION array_intersect(anyarray, anyarray)
  RETURNS anyarray
  language sql
as $FUNCTION$
    SELECT ARRAY(
        SELECT UNNEST($1)
        INTERSECT
        SELECT UNNEST($2)
    );
$FUNCTION$;

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
-- Get average tone of unique articles;
-- Continue calculating additional info. eg "num_unique_articles"
SELECT
  date, country, from_onshore,
  AVG(tone) as avg_tone,
  SUM(num_unique_articles) as num_unique_articles,
  COUNT(*) as num_unique_articles_relevant,
  AVG(num_themes) as avg_num_themes,
  AVG(num_relevant_themes) as avg_num_relevant_themes
FROM
  (
    -- Combine (intra-country) syndicated articles into one row;
    -- Begin calculating additional info. eg "num_unique_articles"
    SELECT
      DATE(datetime) as date, country, from_onshore,
      AVG(tone) as tone,
      COUNT(*) as num_unique_articles,
      AVG(num_themes) as num_themes,
      AVG(num_relevant_themes) as num_relevant_themes
    FROM
      (
        -- Construct "from_onshore"; Filter by prop. of themes that are relevant
        SELECT
          *,
          CASE
            WHEN country = 'AS' then source_name LIKE '%.au%'
            ELSE source_name LIKE '%.' || LOWER(country) || '%'
          END as from_onshore
        FROM
          (
            -- Expand "countries"; Construct "num_themes", "num_relevant_themes"
            SELECT
              *,
              UNNEST(countries) as country,
              CARDINALITY(themes) as num_themes,
              CARDINALITY(array_intersect(
                themes_ref.housing,
                themes
              )) as num_relevant_themes
            FROM
              gdelt_raw, themes_ref
            WHERE
              DATE(datetime) > '2021-07-26' -- FOR QUICKER TESTING
          ) t1
        WHERE
          num_themes > 0 AND 1.0 * num_relevant_themes / num_themes > 0.1
      ) t2
    GROUP BY
      date,
      country, from_onshore,
      pos, neg, wc  -- to control for syndication
  ) t3
GROUP BY
  date, country, from_onshore
ORDER BY
  date, country, from_onshore
;

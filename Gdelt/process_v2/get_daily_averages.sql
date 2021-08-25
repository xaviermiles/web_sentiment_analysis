/*
Gets daily average tone and article count, broken down by country mentioned
(country), themes/overall (theme), and where the article was published
(from_onshore).


Controls for syndication by grouping articles that have the same publish date,
positive score, negative score, and word count. There is two main limitations
to the current implementation, which are expected to have minor impacts on the
resulting data.

Limitations:
1) Doesn't adjust for articles syndicated within country on different dates.

This is inherrent to the date+pos+neg+wc grouping. Could group just by
pos+neg+wc, but this increases the chance of two similar-but-different articles
(from vastly different dates) being marked as duplicates.

2) Doesn't adjust for articles syndicated between countries of interest at all.

This logic *should* be possible for same-date inter-country republication. But
this would be complicated since it requires determining which of the articles
was published first to determine where the article was originally published (for
"from_onshore" column).
*/

CREATE OR REPLACE FUNCTION array_intersect(anyarray, anyarray)
  RETURNS anyarray
  language sql
AS $FUNCTION$
    SELECT ARRAY(
        SELECT UNNEST($1)
        INTERSECT
        SELECT UNNEST($2)
    );
$FUNCTION$;

CREATE TABLE IF NOT EXISTS daily_tone (
    date         DATE,
    country      VARCHAR(2),
    from_onshore BOOLEAN,
    theme        TEXT,
    avg_tone     NUMERIC,
    num_articles INTEGER,
    PRIMARY KEY (date, country, from_onshore, theme)
);

-- Compute average tones for dates not already in "daily_tone"
-- NB: Doesn't include min/max dates as these are potentially incomplete.
-- Q/  CAN/SHOULD THIS BE CHANGED SO THAT IT ADJUSTS FOR TIMEZONES?
INSERT INTO daily_tone(date, country, from_onshore, theme, avg_tone, num_articles)
SELECT date,
       country,
       from_onshore,
       theme,
       Avg(tone) AS avg_tone,
       Count(*)  AS num_articles
FROM (
    -- Combine (intra-country) syndicated articles into one row
    SELECT date,
           country,
           from_onshore,
           theme,
           Avg(tone)      AS tone
    FROM (
        -- Construct "from_onshore" (for each country)
        SELECT *,
               CASE
                 WHEN country = 'AS' THEN source_name LIKE '%.au%'
                 ELSE source_name LIKE '%.' || Lower(country) || '%'
               END AS from_onshore
        FROM (
            -- Expand "countries", for rows with sufficient relevant themes
            SELECT *,
                   ref_high_level AS theme,
                   Unnest(countries) AS country
            FROM (
                -- Calculate proportion of relevant themes (for each theme)
                SELECT *,
                       Date(datetime) AS date,
                       CASE
                          WHEN ref_high_level = 'ALL' THEN 1.0
                          WHEN Cardinality(themes) = 0 THEN 0.0
                          ELSE 1.0 * Cardinality(array_intersect(ref_low_levels, themes)) / Cardinality(themes)
                       END AS prop_relevant_themes
                FROM (
                    -- Take all combinations of gdelt_raw and high/low themes,
                    -- for any 'new' dates that are not the min/max dates
                    SELECT gdelt_raw.*,
                           themes_ref.high_level AS ref_high_level,
                           themes_ref.low_levels AS ref_low_levels
                    FROM   gdelt_raw,
                           themes_ref,
                           (
                              SELECT Date(datetime) AS min_date
                              FROM gdelt_raw
                              ORDER BY gkg_id ASC
                              LIMIT 1
                           ) min_table,
                           (
                              SELECT Date(datetime) AS max_date
                              FROM gdelt_raw
                              ORDER BY gkg_id DESC
                              LIMIT 1
                           ) max_table
                    WHERE  Date(gdelt_raw.datetime) > min_table.min_date AND
                           Date(gdelt_raw.datetime) < max_table.max_date AND
                           --Date(gdelt_raw.datetime) >= '2021-08-01' AND -- FOR TESTING
                           NOT EXISTS (
                              SELECT
                              FROM daily_tone
                              WHERE date = Date(gdelt_raw.datetime)
                           ) -- don't compute for dates already in daily_tone
                ) gdelt_raw_w_themes
            ) t1
            WHERE  prop_relevant_themes > 0.1 -- theme threshold
        ) t2
    ) t3
    GROUP  BY date,
              country,
              from_onshore,
              theme,
              pos, neg, wc -- to control for syndication
) t4
GROUP  BY date,
          country,
          from_onshore,
          theme
ORDER  BY date,
          country,
          from_onshore,
          theme;

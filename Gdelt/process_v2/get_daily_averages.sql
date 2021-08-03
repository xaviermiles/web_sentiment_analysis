-- NB: doesn't adjust for articles syndicated between countries of interest
SELECT
  date, country, from_onshore,
  AVG(tone) as avg_tone
FROM
  (
    -- Combine (intra-country) syndicated articles into one row
    SELECT
      DATE(datetime) as date, country,
      from_onshore,
      AVG(tone) as tone,
      pos, neg, wc
    FROM
      (
        -- Construct "from_onshore"
        SELECT
          *,
          CASE
            WHEN country = 'AS' then source_name LIKE '%.au%'
            ELSE source_name LIKE '%.' || LOWER(country) || '%'
          END as from_onshore
        FROM
          (
            -- Expand "countries"
            SELECT
              *, UNNEST(countries) as country
            FROM
              gdelt_raw
            WHERE DATE(datetime) > '2021-07-26'  -- REMOVE (for testing)
          ) t1
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

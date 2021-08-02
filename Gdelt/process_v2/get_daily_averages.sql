SELECT
  date, country,
  CASE
    WHEN country = 'AS' then source_name LIKE '%.au%'
    ELSE source_name LIKE '%.' || LOWER(country) || '%'
  END as from_onshore,  -- to weight onshore vs offshore articles differently
  AVG(tone) as tone
FROM
  (
    SELECT
      DATE(datetime) as date, UNNEST(countries) as country,
      source_name,
      AVG(tone) as tone,
      pos, neg, wc
    FROM
      gdelt_raw
    GROUP BY
      date, country,
      source_name,
      pos, neg, wc  -- to control for syndication
  ) t1
GROUP BY
  date, country, from_onshore
ORDER BY
  date, country, from_onshore
;

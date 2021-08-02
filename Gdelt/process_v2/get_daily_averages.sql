SELECT
  date, countries, AVG(tone) as avg_tone
FROM
  (
    SELECT
      DATE(datetime) as date, UNNEST(countries) as countries,
      AVG(tone) as tone,
      pos, neg, wc
    FROM
      gdelt_raw
    GROUP BY
      date, countries,
      pos, neg, wc  -- to control for syndication
  ) t1
GROUP BY
  date, countries
ORDER BY
  date, countries
;

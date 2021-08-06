-- NB: doesn't adjust for articles syndicated between countries of interest
SELECT date,
       country,
       from_onshore,
       Avg(tone) AS avg_tone,
       Count(*)  AS num_articles
FROM   (
    -- Combine (intra-country) syndicated articles into one row
    SELECT Date(datetime) AS date,
          country,
          from_onshore,
          Avg(tone)      AS tone
    FROM (
        -- Construct "from_onshore"
        SELECT *,
               CASE
                 WHEN country = 'AS' THEN source_name LIKE '%.au%'
                 ELSE source_name LIKE '%.' || Lower(country) || '%'
               END AS from_onshore
        FROM (
            -- Expand "countries"
            SELECT *,
                   Unnest(countries) AS country
            FROM   gdelt_raw
            WHERE  Date(datetime) > '2021-07-26' -- FOR QUICKER TESTING
        ) t1
    ) t2
    GROUP BY date,
             country,
             from_onshore,
             pos, neg, wc -- to control for syndication
) t3
GROUP  BY date,
          country,
          from_onshore
ORDER  BY date,
          country,
          from_onshore;

WITH aggregated_data AS (
    SELECT 
        toHour(dt) AS hour,
        phrase,
        max(views) AS max_views
    FROM 
        phrases_views
    WHERE 
        toDate(dt) = today()
        AND campaign_id = 1111111
    GROUP BY
        phrase, hour
),
views_with_diff AS (
    SELECT 
        a.phrase,
        a.hour,
        a.max_views,
        COALESCE(b.max_views, 0) AS previous_max_views,
        a.max_views - COALESCE(b.max_views, 0) AS views_diff
    FROM 
        aggregated_data a
    LEFT JOIN
        aggregated_data b
    ON 
        a.phrase = b.phrase AND a.hour = b.hour + 1
)
SELECT
    phrase,
    arrayMap(x -> (toString(x.1), x.2), 
        arraySlice(
            arraySort(x -> -x.1, groupArray((hour, views_diff))), 2, length(groupArray((hour, views_diff))) - 2
        )
    ) AS views_by_hour
FROM
    views_with_diff
GROUP BY
    phrase
ORDER BY
    phrase
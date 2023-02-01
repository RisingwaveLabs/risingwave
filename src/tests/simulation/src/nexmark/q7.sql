-- Covers self-join.

CREATE MATERIALIZED VIEW nexmark_q7
AS
SELECT
    B.auction,
    B.price,
    B.bidder,
    B.date_time
FROM
    bid B
JOIN (
    SELECT
        MAX(price) AS maxprice,
        window_end as date_time
    FROM
        TUMBLE(bid, date_time, INTERVAL '10' SECOND)
    GROUP BY
        window_end
) B1 ON B.price = B1.maxprice
WHERE
    B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND
    AND B1.date_time;

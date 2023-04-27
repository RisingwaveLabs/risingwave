-- noinspection SqlNoDataSourceInspectionForFile
-- noinspection SqlResolveForFile
CREATE SINK nexmark_q18 AS
SELECT auction, bidder, price, channel, url, date_time
FROM (SELECT *,
             ROW_NUMBER() OVER (
                 PARTITION BY bidder, auction
                 ORDER BY date_time DESC
                 ) AS rank_number
      FROM bid)
WHERE rank_number <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');

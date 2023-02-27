CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT DATE '2022-11-17' AS col_0, (BIGINT '224') AS col_1, '1nfMfPSV8u' AS col_2 FROM orders AS t_0 LEFT JOIN bid AS t_1 ON t_0.o_clerk = t_1.channel GROUP BY t_1.auction, t_0.o_orderstatus, t_1.date_time, t_0.o_clerk, t_0.o_totalprice, t_1.price, t_0.o_orderpriority, t_1.url;
CREATE MATERIALIZED VIEW m1 AS SELECT sq_2.col_1 AS col_0, 'GmUaL4q7kh' AS col_1 FROM (SELECT t_1.c_phone AS col_0, ((INT '343') | (BIGINT '295')) AS col_1, (CASE WHEN true THEN t_1.c_phone WHEN (TIME '02:27:48' > TIME '02:27:47') THEN t_0.extra WHEN false THEN 'xkIfhQJsEd' ELSE (TRIM(t_0.extra)) END) AS col_2 FROM bid AS t_0 FULL JOIN customer AS t_1 ON t_0.extra = t_1.c_phone WHERE true GROUP BY t_0.url, t_0.bidder, t_1.c_comment, t_0.extra, t_1.c_nationkey, t_0.date_time, t_1.c_phone) AS sq_2 WHERE true GROUP BY sq_2.col_1, sq_2.col_0;
CREATE MATERIALIZED VIEW m3 AS SELECT 'S3PslYwNBZ' AS col_0 FROM region AS t_0 LEFT JOIN alltypes2 AS t_1 ON t_0.r_regionkey = t_1.c3 AND t_1.c1 GROUP BY t_0.r_comment HAVING false;
CREATE MATERIALIZED VIEW m5 AS SELECT hop_0.price AS col_0, hop_0.extra AS col_1, hop_0.extra AS col_2, hop_0.auction AS col_3 FROM hop(bid, bid.date_time, INTERVAL '3600', INTERVAL '43200') AS hop_0 GROUP BY hop_0.price, hop_0.extra, hop_0.auction;
CREATE MATERIALIZED VIEW m6 AS WITH with_0 AS (SELECT (((SMALLINT '677') >> (INT '2147483647')) * (coalesce(NULL, NULL, NULL, (BIGINT '887'), NULL, NULL, NULL, NULL, NULL, NULL))) AS col_0, t_2.col_0 AS col_1, t_2.col_0 AS col_2 FROM alltypes2 AS t_1 FULL JOIN m3 AS t_2 ON t_1.c9 = t_2.col_0 WHERE true GROUP BY t_1.c2, t_2.col_0, t_1.c14, t_1.c4 HAVING true) SELECT ARRAY[(SMALLINT '449'), (SMALLINT '-32768'), (SMALLINT '771')] AS col_0 FROM with_0 WHERE false;
CREATE MATERIALIZED VIEW m7 AS SELECT (INT '2147483647') AS col_0, hop_0.c3 AS col_1, (BIGINT '37') AS col_2, ((hop_0.c3 # (((((SMALLINT '545') / (SMALLINT '-19456')) % (SMALLINT '858')) / hop_0.c3) << hop_0.c3)) / (SMALLINT '782')) AS col_3 FROM hop(alltypes1, alltypes1.c11, INTERVAL '86400', INTERVAL '3974400') AS hop_0 GROUP BY hop_0.c3;
CREATE MATERIALIZED VIEW m8 AS SELECT t_1.col_1 AS col_0, (2147483647) AS col_1 FROM partsupp AS t_0 JOIN m1 AS t_1 ON t_0.ps_comment = t_1.col_1 AND true WHERE ((FLOAT '423') = (SMALLINT '704')) GROUP BY t_0.ps_partkey, t_1.col_0, t_1.col_1 HAVING true;
CREATE MATERIALIZED VIEW m9 AS SELECT (t_0.col_1 - (INT '229')) AS col_0 FROM m7 AS t_0 FULL JOIN m5 AS t_1 ON t_0.col_2 = t_1.col_3 WHERE false GROUP BY t_0.col_1, t_1.col_0 HAVING true;

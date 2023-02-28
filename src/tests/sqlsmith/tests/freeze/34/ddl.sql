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
CREATE MATERIALIZED VIEW m0 AS SELECT hop_0.id AS col_0, hop_0.initial_bid AS col_1, (534) AS col_2, (BIGINT '9223372036854775807') AS col_3 FROM hop(auction, auction.date_time, INTERVAL '86400', INTERVAL '8380800') AS hop_0 WHERE false GROUP BY hop_0.initial_bid, hop_0.id, hop_0.date_time HAVING ((916) = (FLOAT '358'));
CREATE MATERIALIZED VIEW m1 AS SELECT sq_3.col_0 AS col_0, 'URLM1KMzj8' AS col_1, (101) AS col_2 FROM (WITH with_0 AS (SELECT t_2.c16 AS col_0, t_2.c13 AS col_1 FROM region AS t_1 LEFT JOIN alltypes1 AS t_2 ON t_1.r_name = t_2.c9 GROUP BY t_2.c10, t_2.c14, t_2.c13, t_1.r_name, t_2.c16, t_2.c3) SELECT (243) AS col_0 FROM with_0 WHERE false) AS sq_3 GROUP BY sq_3.col_0;
CREATE MATERIALIZED VIEW m2 AS SELECT tumble_0.date_time AS col_0, TIMESTAMP '2022-09-30 18:49:12' AS col_1, (tumble_0.date_time + (INTERVAL '0')) AS col_2 FROM tumble(person, person.date_time, INTERVAL '58') AS tumble_0 WHERE true GROUP BY tumble_0.date_time HAVING false;
CREATE MATERIALIZED VIEW m3 AS SELECT (FLOAT '-1303426337') AS col_0, t_1.c6 AS col_1, (CAST(NULL AS STRUCT<a INT>)) AS col_2 FROM region AS t_0 LEFT JOIN alltypes1 AS t_1 ON t_0.r_regionkey = t_1.c3 WHERE (CASE WHEN ((t_1.c3 & t_1.c2) >= ((SMALLINT '0') / t_1.c7)) THEN (true) WHEN t_1.c1 THEN (t_1.c5 > t_1.c5) WHEN false THEN t_1.c1 ELSE (false) END) GROUP BY t_1.c11, t_1.c16, t_1.c7, t_1.c6, t_1.c8, t_1.c14, t_1.c4 HAVING false;
CREATE MATERIALIZED VIEW m4 AS SELECT 'GUMM4sM70x' AS col_0, tumble_0.extra AS col_1 FROM tumble(person, person.date_time, INTERVAL '14') AS tumble_0 WHERE false GROUP BY tumble_0.state, tumble_0.extra;
CREATE MATERIALIZED VIEW m6 AS SELECT ((SMALLINT '145') # (approx_count_distinct((TIMESTAMP '2022-09-30 17:49:13')) FILTER(WHERE true) | tumble_0.category)) AS col_0, tumble_0.description AS col_1, tumble_0.category AS col_2 FROM tumble(auction, auction.expires, INTERVAL '30') AS tumble_0 GROUP BY tumble_0.category, tumble_0.description HAVING false;
CREATE MATERIALIZED VIEW m7 AS WITH with_0 AS (WITH with_1 AS (SELECT t_3.col_2 AS col_0, t_2.state AS col_1, t_2.extra AS col_2 FROM person AS t_2 LEFT JOIN m1 AS t_3 ON t_2.city = t_3.col_1 GROUP BY t_3.col_2, t_2.extra, t_2.email_address, t_2.state HAVING true) SELECT (INTERVAL '0') AS col_0, 'l11XkwkoOV' AS col_1 FROM with_1 WHERE false) SELECT ((FLOAT '0')) AS col_0, (SMALLINT '-32768') AS col_1, (SMALLINT '229') AS col_2, false AS col_3 FROM with_0;
CREATE MATERIALIZED VIEW m8 AS SELECT (FLOAT '534') AS col_0, t_1.extra AS col_1 FROM alltypes1 AS t_0 RIGHT JOIN person AS t_1 ON t_0.c4 = t_1.id AND ((REAL '884') >= (BIGINT '543')) GROUP BY t_1.city, t_0.c15, t_0.c10, t_0.c14, t_1.extra, t_0.c9, t_1.id, t_0.c16 HAVING (false);
CREATE MATERIALIZED VIEW m9 AS WITH with_0 AS (SELECT t_3.col_1 AS col_0, (BIGINT '966') AS col_1, t_3.col_0 AS col_2 FROM m0 AS t_3 WHERE false GROUP BY t_3.col_0, t_3.col_1) SELECT (335) AS col_0, (INTERVAL '-1') AS col_1, true AS col_2, (486) AS col_3 FROM with_0 WHERE ((REAL '546') <= ((BIGINT '1') / (2147483647)));

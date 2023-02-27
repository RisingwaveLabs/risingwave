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
CREATE MATERIALIZED VIEW m0 AS SELECT ((INT '-1842480136') # (BIGINT '278')) AS col_0, (sq_2.col_0 / sq_2.col_0) AS col_1 FROM (SELECT t_1.id AS col_0, t_1.id AS col_1, t_0.c16 AS col_2, ((120131714) / (SMALLINT '477')) AS col_3 FROM alltypes1 AS t_0 FULL JOIN person AS t_1 ON t_0.c9 = t_1.city WHERE t_0.c1 GROUP BY t_0.c16, t_1.date_time, t_0.c9, t_1.email_address, t_1.id, t_0.c8, t_0.c3, t_0.c15 HAVING true) AS sq_2 WHERE false GROUP BY sq_2.col_0;
CREATE MATERIALIZED VIEW m1 AS SELECT sq_2.col_0 AS col_0, ARRAY[TIMESTAMP '2022-08-27 17:25:18'] AS col_1, ARRAY['fMQAktvOHf', 'w9bsbPGAhI', 'eUlXFIBWMq', 'KFHMFkye42'] AS col_2, sq_2.col_0 AS col_3 FROM (SELECT (split_part((lower((TRIM(LEADING ('BL7EY6g2Sp') FROM (substr(t_0.o_clerk, (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, (INT '287'), NULL, NULL)))))))), t_1.r_name, t_0.o_shippriority)) AS col_0 FROM orders AS t_0 JOIN region AS t_1 ON t_0.o_clerk = t_1.r_comment WHERE false GROUP BY t_1.r_comment, t_1.r_regionkey, t_0.o_custkey, t_1.r_name, t_0.o_clerk, t_0.o_shippriority) AS sq_2 GROUP BY sq_2.col_0;
CREATE MATERIALIZED VIEW m2 AS SELECT t_1.r_name AS col_0, 'mWr3Pk12YS' AS col_1, t_1.r_name AS col_2, DATE '2022-09-03' AS col_3 FROM region AS t_0 RIGHT JOIN region AS t_1 ON t_0.r_name = t_1.r_comment WHERE false GROUP BY t_1.r_regionkey, t_1.r_name, t_0.r_regionkey HAVING true;
CREATE MATERIALIZED VIEW m3 AS SELECT t_0.c14 AS col_0, (INTERVAL '-3600') AS col_1 FROM alltypes2 AS t_0 RIGHT JOIN nation AS t_1 ON t_0.c9 = t_1.n_name AND true WHERE t_0.c1 GROUP BY t_0.c13, t_0.c15, t_0.c14, t_0.c3, t_0.c11, t_0.c2;
CREATE MATERIALIZED VIEW m4 AS SELECT max(DATE '2022-08-27') FILTER(WHERE ((BIGINT '608') < (INT '818'))) AS col_0 FROM lineitem AS t_0 GROUP BY t_0.l_commitdate, t_0.l_comment, t_0.l_linenumber, t_0.l_quantity;
CREATE MATERIALIZED VIEW m5 AS SELECT (FLOAT '481') AS col_0, (INT '-2147483648') AS col_1, (FLOAT '569') AS col_2, hop_0.id AS col_3 FROM hop(person, person.date_time, INTERVAL '604800', INTERVAL '50198400') AS hop_0 WHERE false GROUP BY hop_0.email_address, hop_0.extra, hop_0.id;
CREATE MATERIALIZED VIEW m6 AS SELECT ((INT '423') - (INT '0')) AS col_0, t_1.id AS col_1, t_1.name AS col_2 FROM orders AS t_0 RIGHT JOIN person AS t_1 ON t_0.o_clerk = t_1.city WHERE false GROUP BY t_0.o_orderkey, t_1.state, t_1.name, t_1.city, t_1.email_address, t_0.o_totalprice, t_0.o_comment, t_1.id, t_1.date_time HAVING true;
CREATE MATERIALIZED VIEW m8 AS SELECT t_1.c13 AS col_0, (t_1.c3 / t_0.c_custkey) AS col_1, t_1.c9 AS col_2, TIME '16:25:21' AS col_3 FROM customer AS t_0 FULL JOIN alltypes2 AS t_1 ON t_0.c_address = t_1.c9 GROUP BY t_0.c_acctbal, t_0.c_comment, t_1.c13, t_1.c4, t_0.c_custkey, t_1.c3, t_1.c9, t_1.c11, t_1.c2, t_1.c14, t_0.c_phone HAVING true;
CREATE MATERIALIZED VIEW m9 AS SELECT (CASE WHEN false THEN (t_0.col_1 + (SMALLINT '19')) WHEN false THEN approx_count_distinct((REAL '-2147483648')) ELSE t_0.col_1 END) AS col_0, (BIGINT '515') AS col_1, t_0.col_1 AS col_2, (((BIGINT '451') - (INT '-152579575')) + (INT '-93226591')) AS col_3 FROM m6 AS t_0 FULL JOIN m8 AS t_1 ON t_0.col_2 = t_1.col_2 AND true GROUP BY t_0.col_1;

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
CREATE MATERIALIZED VIEW m0 AS SELECT sq_1.col_0 AS col_0, true AS col_1, sq_1.col_0 AS col_2 FROM (SELECT ARRAY['T09OrtkvSH'] AS col_0 FROM tumble(alltypes2, alltypes2.c11, INTERVAL '18') AS tumble_0 GROUP BY tumble_0.c14, tumble_0.c8, tumble_0.c1, tumble_0.c2, tumble_0.c16, tumble_0.c10) AS sq_1 WHERE true GROUP BY sq_1.col_0;
CREATE MATERIALIZED VIEW m1 AS SELECT sq_2.col_1 AS col_0 FROM (SELECT t_1.n_name AS col_0, t_0.reserve AS col_1, t_1.n_nationkey AS col_2 FROM auction AS t_0 LEFT JOIN nation AS t_1 ON t_0.extra = t_1.n_name GROUP BY t_0.item_name, t_1.n_nationkey, t_1.n_name, t_0.reserve) AS sq_2 WHERE (DATE '2022-06-17' > (sq_2.col_2 + DATE '2022-06-18')) GROUP BY sq_2.col_1 HAVING false;
CREATE MATERIALIZED VIEW m2 AS SELECT t_2.p_retailprice AS col_0 FROM part AS t_2 GROUP BY t_2.p_brand, t_2.p_type, t_2.p_retailprice HAVING (coalesce(NULL, NULL, NULL, NULL, false, NULL, NULL, NULL, NULL, NULL));
CREATE MATERIALIZED VIEW m3 AS WITH with_0 AS (SELECT t_1.id AS col_0, t_1.id AS col_1, (TRIM((TRIM(TRAILING t_1.credit_card FROM 'mvNTok7xPS')))) AS col_2, t_1.name AS col_3 FROM person AS t_1 GROUP BY t_1.name, t_1.id, t_1.city, t_1.credit_card) SELECT ARRAY[(REAL '405')] AS col_0, TIMESTAMP '2022-06-11 13:09:05' AS col_1 FROM with_0 WHERE true;
CREATE MATERIALIZED VIEW m5 AS WITH with_0 AS (SELECT 'zuqtbCRunw' AS col_0, (substr(sq_4.col_1, ((INT '526') >> (SMALLINT '21770')), (char_length('6HmPYthjmF')))) AS col_1, (TRIM((to_char(TIMESTAMP '2022-06-17 02:43:08', sq_4.col_1)))) AS col_2 FROM (SELECT 'EBdBuP1BDJ' AS col_0, (TRIM(sq_3.col_0)) AS col_1, sq_3.col_0 AS col_2, DATE '2022-06-18' AS col_3 FROM (SELECT t_1.c_mktsegment AS col_0, t_1.c_name AS col_1, t_1.c_mktsegment AS col_2, (INT '336') AS col_3 FROM customer AS t_1 RIGHT JOIN nation AS t_2 ON t_1.c_comment = t_2.n_comment AND (false IS FALSE) GROUP BY t_1.c_name, t_2.n_nationkey, t_2.n_comment, t_1.c_mktsegment HAVING true) AS sq_3 WHERE false GROUP BY sq_3.col_0 HAVING (CASE WHEN true THEN CAST((INT '100') AS BOOLEAN) WHEN false THEN true WHEN CAST(min((INT '364')) FILTER(WHERE false) AS BOOLEAN) THEN CAST((INT '456') AS BOOLEAN) ELSE false END)) AS sq_4 WHERE true GROUP BY sq_4.col_3, sq_4.col_1) SELECT 'pWFX9u0KVl' AS col_0, ((REAL '1028253788') <> (BIGINT '-9223372036854775808')) AS col_1, true AS col_2, (INT '763') AS col_3 FROM with_0;
CREATE MATERIALIZED VIEW m6 AS SELECT (OVERLAY((TRIM(LEADING (split_part((TRIM((TRIM(t_0.s_address)))), t_0.s_address, (SMALLINT '678'))) FROM (to_char(TIMESTAMP '2022-06-11 02:43:09', t_1.credit_card)))) PLACING (OVERLAY(t_1.name PLACING t_0.s_address FROM (INT '1077314191') FOR ((SMALLINT '219') * (INT '581')))) FROM ((INT '533') * (INT '898')))) AS col_0, t_1.name AS col_1, (REAL '97') AS col_2 FROM supplier AS t_0 JOIN person AS t_1 ON t_0.s_comment = t_1.extra AND (true) WHERE false GROUP BY t_0.s_name, t_0.s_address, t_0.s_phone, t_1.name, t_1.credit_card;
CREATE MATERIALIZED VIEW m7 AS SELECT 'Y0tml0yO8t' AS col_0, ((FLOAT '128')) AS col_1, t_2.col_1 AS col_2, t_2.col_1 AS col_3 FROM m3 AS t_2 WHERE ((FLOAT '1') <> (SMALLINT '778')) GROUP BY t_2.col_1 HAVING false;
CREATE MATERIALIZED VIEW m8 AS SELECT (TRIM(t_0.l_returnflag)) AS col_0 FROM lineitem AS t_0 FULL JOIN bid AS t_1 ON t_0.l_comment = t_1.url WHERE true GROUP BY t_0.l_extendedprice, t_0.l_returnflag, t_1.url HAVING (false);
CREATE MATERIALIZED VIEW m9 AS SELECT t_1.email_address AS col_0 FROM m5 AS t_0 JOIN person AS t_1 ON t_0.col_0 = t_1.credit_card GROUP BY t_1.email_address HAVING false;

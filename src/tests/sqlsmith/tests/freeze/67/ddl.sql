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
CREATE MATERIALIZED VIEW m0 AS SELECT sq_1.col_0 AS col_0, sq_1.col_0 AS col_1 FROM (SELECT t_0.o_clerk AS col_0, t_0.o_comment AS col_1 FROM orders AS t_0 GROUP BY t_0.o_comment, t_0.o_orderdate, t_0.o_orderkey, t_0.o_shippriority, t_0.o_clerk) AS sq_1 WHERE false GROUP BY sq_1.col_0;
CREATE MATERIALIZED VIEW m1 AS SELECT (sq_1.col_3 & (SMALLINT '169')) AS col_0 FROM (SELECT hop_0.c8 AS col_0, hop_0.c2 AS col_1, hop_0.c8 AS col_2, hop_0.c2 AS col_3 FROM hop(alltypes1, alltypes1.c11, INTERVAL '27049', INTERVAL '243441') AS hop_0 WHERE hop_0.c1 GROUP BY hop_0.c8, hop_0.c2, hop_0.c10) AS sq_1 WHERE (false) GROUP BY sq_1.col_0, sq_1.col_3;
CREATE MATERIALIZED VIEW m2 AS SELECT t_1.c11 AS col_0, (FLOAT '233') AS col_1, (t_1.c10 - (INTERVAL '-604800')) AS col_2 FROM nation AS t_0 RIGHT JOIN alltypes1 AS t_1 ON t_0.n_name = t_1.c9 AND (t_1.c7 <= t_1.c7) WHERE t_1.c1 GROUP BY t_0.n_nationkey, t_1.c15, t_1.c5, t_1.c10, t_1.c14, t_1.c3, t_1.c11;
CREATE MATERIALIZED VIEW m3 AS SELECT t_1.s_comment AS col_0, t_0.c_nationkey AS col_1 FROM customer AS t_0 FULL JOIN supplier AS t_1 ON t_0.c_name = t_1.s_name WHERE false GROUP BY t_0.c_nationkey, t_1.s_comment, t_0.c_name, t_1.s_address HAVING true;
CREATE MATERIALIZED VIEW m5 AS SELECT hop_0.c3 AS col_0 FROM hop(alltypes1, alltypes1.c11, INTERVAL '60', INTERVAL '1260') AS hop_0 WHERE hop_0.c1 GROUP BY hop_0.c3, hop_0.c1, hop_0.c13 HAVING hop_0.c1;
CREATE MATERIALIZED VIEW m6 AS SELECT t_1.credit_card AS col_0 FROM supplier AS t_0 FULL JOIN person AS t_1 ON t_0.s_name = t_1.state GROUP BY t_0.s_name, t_1.email_address, t_1.credit_card HAVING ((INT '-2147483648') = (coalesce((- (FLOAT '263471041')), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)));
CREATE MATERIALIZED VIEW m7 AS WITH with_0 AS (SELECT ((- (612)) * t_1.c13) AS col_0 FROM alltypes1 AS t_1 LEFT JOIN nation AS t_2 ON t_1.c3 = t_2.n_regionkey AND true GROUP BY t_1.c13) SELECT (BIGINT '678') AS col_0 FROM with_0;

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
CREATE MATERIALIZED VIEW m0 AS SELECT tumble_0.email_address AS col_0 FROM tumble(person, person.date_time, INTERVAL '93') AS tumble_0 GROUP BY tumble_0.email_address HAVING false;
CREATE MATERIALIZED VIEW m1 AS SELECT t_1.ps_availqty AS col_0, (t_1.ps_availqty + t_0.c3) AS col_1, '08Bj7IYeYf' AS col_2 FROM alltypes1 AS t_0 JOIN partsupp AS t_1 ON t_0.c3 = t_1.ps_suppkey GROUP BY t_0.c2, t_0.c15, t_1.ps_availqty, t_0.c16, t_1.ps_supplycost, t_0.c3, t_0.c5, t_0.c9 HAVING true;
CREATE MATERIALIZED VIEW m2 AS WITH with_0 AS (WITH with_1 AS (SELECT 'Uo5ttZFIyP' AS col_0, CAST(t_3.ps_partkey AS BOOLEAN) AS col_1, t_3.ps_suppkey AS col_2 FROM nation AS t_2 FULL JOIN partsupp AS t_3 ON t_2.n_nationkey = t_3.ps_suppkey WHERE true GROUP BY t_3.ps_partkey, t_3.ps_suppkey) SELECT ((BIGINT '592')) AS col_0 FROM with_1) SELECT (INT '952') AS col_0, '9dDyv84Zrl' AS col_1, false AS col_2 FROM with_0 WHERE false;
CREATE MATERIALIZED VIEW m3 AS SELECT 'mcGoExMWj6' AS col_0, t_0.item_name AS col_1, (CASE WHEN true THEN t_0.expires ELSE t_0.expires END) AS col_2 FROM auction AS t_0 WHERE ((INT '-1704660683') = (FLOAT '10')) GROUP BY t_0.item_name, t_0.description, t_0.expires;
CREATE MATERIALIZED VIEW m4 AS SELECT (168) AS col_0, (INTERVAL '-604800') AS col_1, ((INT '848') * sq_1.col_0) AS col_2, sq_1.col_0 AS col_3 FROM (SELECT tumble_0.c13 AS col_0, ((REAL '2147483647')) AS col_1, tumble_0.c6 AS col_2, ((FLOAT '-1384402396')) AS col_3 FROM tumble(alltypes2, alltypes2.c11, INTERVAL '32') AS tumble_0 GROUP BY tumble_0.c6, tumble_0.c8, tumble_0.c13, tumble_0.c7, tumble_0.c5, tumble_0.c2) AS sq_1 WHERE (((69)) = sq_1.col_1) GROUP BY sq_1.col_0, sq_1.col_1;
CREATE MATERIALIZED VIEW m5 AS WITH with_0 AS (SELECT (INT '12') AS col_0, ARRAY[(INT '362')] AS col_1, ((SMALLINT '943') * t_1.r_regionkey) AS col_2, t_1.r_regionkey AS col_3 FROM region AS t_1 LEFT JOIN region AS t_2 ON t_1.r_comment = t_2.r_name AND true WHERE true GROUP BY t_1.r_regionkey HAVING false) SELECT true AS col_0, ((INT '597') % (BIGINT '556')) AS col_1 FROM with_0;
CREATE MATERIALIZED VIEW m6 AS SELECT tumble_0.category AS col_0, tumble_0.category AS col_1, tumble_0.category AS col_2, (-1646738767) AS col_3 FROM tumble(auction, auction.expires, INTERVAL '37') AS tumble_0 GROUP BY tumble_0.category;
CREATE MATERIALIZED VIEW m7 AS SELECT CAST(NULL AS STRUCT<a INT>) AS col_0, tumble_0.c14 AS col_1 FROM tumble(alltypes1, alltypes1.c11, INTERVAL '99') AS tumble_0 GROUP BY tumble_0.c4, tumble_0.c7, tumble_0.c8, tumble_0.c9, tumble_0.c14, tumble_0.c13, tumble_0.c11;
CREATE MATERIALIZED VIEW m8 AS SELECT ARRAY[(REAL '473'), (REAL '218'), (REAL '764'), (REAL '451')] AS col_0, (TIMESTAMP '2022-07-29 05:08:21') AS col_1, t_0.c6 AS col_2 FROM alltypes1 AS t_0 GROUP BY t_0.c6, t_0.c5, t_0.c11, t_0.c8;
CREATE MATERIALIZED VIEW m9 AS SELECT tumble_0.col_2 AS col_0, ((51) / (790)) AS col_1, (FLOAT '864') AS col_2, (((char_length('esp5PY6vP5')) % (INT '351')) IS NULL) AS col_3 FROM tumble(m8, m8.col_1, INTERVAL '75') AS tumble_0 WHERE false GROUP BY tumble_0.col_2;

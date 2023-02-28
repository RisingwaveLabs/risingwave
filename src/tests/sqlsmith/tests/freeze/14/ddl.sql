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
CREATE MATERIALIZED VIEW m0 AS WITH with_0 AS (SELECT (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, (INT '782'), NULL)) AS col_0, CAST(true AS INT) AS col_1, CAST(false AS INT) AS col_2 FROM customer AS t_1 WHERE false GROUP BY t_1.c_custkey) SELECT (BIGINT '9223372036854775807') AS col_0, TIMESTAMP '2022-12-03 01:28:35' AS col_1, DATE '2022-11-23' AS col_2, (INTERVAL '-1') AS col_3 FROM with_0 WHERE true;
CREATE MATERIALIZED VIEW m1 AS SELECT t_0.p_mfgr AS col_0, (upper('TvzPqI4Hh3')) AS col_1, t_0.p_mfgr AS col_2, t_0.p_mfgr AS col_3 FROM part AS t_0 WHERE true GROUP BY t_0.p_mfgr;
CREATE MATERIALIZED VIEW m2 AS SELECT t_0.ps_comment AS col_0 FROM partsupp AS t_0 RIGHT JOIN bid AS t_1 ON t_0.ps_comment = t_1.channel AND true GROUP BY t_0.ps_comment, t_1.channel;
CREATE MATERIALIZED VIEW m3 AS SELECT (ARRAY['n2Bbsdwnqy', 'oPlq0Zy80T', 'GeS9fxFrkL', 'qegVgm0yfd']) AS col_0, (to_char(TIMESTAMP '2022-12-01 06:52:50', ('rcR59fl7UK'))) AS col_1, t_1.credit_card AS col_2 FROM alltypes2 AS t_0 JOIN person AS t_1 ON t_0.c4 = t_1.id WHERE true GROUP BY t_1.email_address, t_0.c7, t_0.c16, t_1.credit_card;
CREATE MATERIALIZED VIEW m4 AS SELECT t_0.initial_bid AS col_0, ('EgnmKLh9bw') AS col_1 FROM auction AS t_0 WHERE false GROUP BY t_0.extra, t_0.initial_bid, t_0.description, t_0.id;
CREATE MATERIALIZED VIEW m5 AS SELECT TIMESTAMP '2022-12-02 01:17:06' AS col_0, (INT '1') AS col_1, hop_0.c10 AS col_2 FROM hop(alltypes1, alltypes1.c11, INTERVAL '86400', INTERVAL '4924800') AS hop_0 GROUP BY hop_0.c11, hop_0.c9, hop_0.c3, hop_0.c10;
CREATE MATERIALIZED VIEW m6 AS SELECT tumble_0.col_2 AS col_0, tumble_0.col_3 AS col_1, (FLOAT '322') AS col_2 FROM tumble(m0, m0.col_1, INTERVAL '84') AS tumble_0 GROUP BY tumble_0.col_3, tumble_0.col_2 HAVING false;
CREATE MATERIALIZED VIEW m7 AS SELECT (tumble_0.col_1 - (INTERVAL '-86400')) AS col_0, tumble_0.col_1 AS col_1, tumble_0.col_1 AS col_2 FROM tumble(m0, m0.col_1, INTERVAL '31') AS tumble_0 WHERE true GROUP BY tumble_0.col_1 HAVING (true);
CREATE MATERIALIZED VIEW m8 AS SELECT min(hop_0.col_1) AS col_0, true AS col_1 FROM hop(m5, m5.col_0, INTERVAL '604800', INTERVAL '22982400') AS hop_0 GROUP BY hop_0.col_0, hop_0.col_1;
CREATE MATERIALIZED VIEW m9 AS WITH with_0 AS (SELECT t_1.o_comment AS col_0, (TRIM(t_1.o_comment)) AS col_1, 'dGLUyFQOmt' AS col_2 FROM orders AS t_1 WHERE true GROUP BY t_1.o_comment, t_1.o_orderpriority, t_1.o_orderstatus HAVING true) SELECT (FLOAT '721') AS col_0, (FLOAT '908') AS col_1, (coalesce(NULL, NULL, NULL, NULL, ((SMALLINT '993') = (INT '319')), NULL, NULL, NULL, NULL, NULL)) AS col_2, DATE '2022-12-02' AS col_3 FROM with_0 WHERE true;

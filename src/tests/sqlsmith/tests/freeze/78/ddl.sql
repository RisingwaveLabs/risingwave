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
CREATE MATERIALIZED VIEW m0 AS SELECT hop_0.c10 AS col_0, hop_0.c10 AS col_1, (hop_0.c3 & hop_0.c3) AS col_2, (SMALLINT '631') AS col_3 FROM hop(alltypes2, alltypes2.c11, INTERVAL '1', INTERVAL '6') AS hop_0 GROUP BY hop_0.c3, hop_0.c9, hop_0.c4, hop_0.c13, hop_0.c1, hop_0.c10, hop_0.c7, hop_0.c8;
CREATE MATERIALIZED VIEW m1 AS SELECT t_0.initial_bid AS col_0 FROM auction AS t_0 RIGHT JOIN nation AS t_1 ON t_0.item_name = t_1.n_name WHERE true GROUP BY t_0.initial_bid, t_0.id, t_0.category, t_1.n_nationkey, t_0.seller HAVING true;
CREATE MATERIALIZED VIEW m2 AS SELECT TIMESTAMP '2022-05-10 22:28:25' AS col_0, tumble_0.bidder AS col_1 FROM tumble(bid, bid.date_time, INTERVAL '33') AS tumble_0 WHERE true GROUP BY tumble_0.date_time, tumble_0.bidder HAVING true;
CREATE MATERIALIZED VIEW m3 AS SELECT (REAL '1629542581') AS col_0, TIME '21:29:25' AS col_1, (BIGINT '31') AS col_2 FROM hop(auction, auction.date_time, INTERVAL '604800', INTERVAL '27820800') AS hop_0 GROUP BY hop_0.description, hop_0.reserve, hop_0.expires, hop_0.category, hop_0.date_time HAVING true;
CREATE MATERIALIZED VIEW m4 AS SELECT sq_2.col_0 AS col_0, sq_2.col_0 AS col_1, sq_2.col_0 AS col_2, sq_2.col_0 AS col_3 FROM (SELECT (INTERVAL '0') AS col_0, ((INT '622') - sq_1.col_0) AS col_1 FROM (SELECT hop_0.col_1 AS col_0, (hop_0.col_1 # (((SMALLINT '-20843') - (SMALLINT '429')) << ((SMALLINT '-3795') >> (SMALLINT '10864')))) AS col_1, hop_0.col_1 AS col_2, ((INT '191') + hop_0.col_1) AS col_3 FROM hop(m2, m2.col_0, INTERVAL '60', INTERVAL '4680') AS hop_0 WHERE (true) GROUP BY hop_0.col_1) AS sq_1 GROUP BY sq_1.col_0) AS sq_2 GROUP BY sq_2.col_0;
CREATE MATERIALIZED VIEW m6 AS SELECT tumble_0.description AS col_0 FROM tumble(auction, auction.expires, INTERVAL '58') AS tumble_0 WHERE true GROUP BY tumble_0.description, tumble_0.initial_bid, tumble_0.extra, tumble_0.date_time HAVING false;
CREATE MATERIALIZED VIEW m7 AS WITH with_0 AS (SELECT '9QHwdGL22h' AS col_0, t_3.r_regionkey AS col_1 FROM region AS t_3 WHERE false GROUP BY t_3.r_comment, t_3.r_regionkey) SELECT (SMALLINT '354') AS col_0, (REAL '1') AS col_1, '76W1fP1ik1' AS col_2, TIMESTAMP '2022-05-03 22:29:27' AS col_3 FROM with_0 WHERE false;
CREATE MATERIALIZED VIEW m8 AS SELECT (INTERVAL '3600') AS col_0, t_0.c_custkey AS col_1, (1) AS col_2, (INTERVAL '60') AS col_3 FROM customer AS t_0 GROUP BY t_0.c_custkey, t_0.c_acctbal, t_0.c_address HAVING false;
CREATE MATERIALIZED VIEW m9 AS SELECT DATE '2022-05-03' AS col_0, DATE '2022-05-02' AS col_1, 'HauT9xSPQZ' AS col_2, t_0.city AS col_3 FROM person AS t_0 GROUP BY t_0.city, t_0.name, t_0.extra, t_0.id HAVING (TIMESTAMP '2022-05-09 22:29:27' = TIMESTAMP '2022-05-09 22:29:27');

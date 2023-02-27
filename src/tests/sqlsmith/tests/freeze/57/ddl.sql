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
CREATE MATERIALIZED VIEW m0 AS SELECT t_0.email_address AS col_0 FROM person AS t_0 WHERE false GROUP BY t_0.email_address HAVING CAST((INT '1') AS BOOLEAN);
CREATE MATERIALIZED VIEW m1 AS SELECT sq_1.col_1 AS col_0, sq_1.col_1 AS col_1, sq_1.col_1 AS col_2 FROM (SELECT hop_0.c15 AS col_0, hop_0.c5 AS col_1, (FLOAT '119') AS col_2 FROM hop(alltypes2, alltypes2.c11, INTERVAL '1', INTERVAL '61') AS hop_0 WHERE (true) GROUP BY hop_0.c15, hop_0.c3, hop_0.c6, hop_0.c5 HAVING false) AS sq_1 GROUP BY sq_1.col_1, sq_1.col_2 HAVING (false);
CREATE MATERIALIZED VIEW m2 AS WITH with_0 AS (SELECT (t_2.price | (INT '851')) AS col_0 FROM region AS t_1 JOIN bid AS t_2 ON t_1.r_comment = t_2.url AND (TIME '23:43:39' <> TIME '23:43:39') WHERE true GROUP BY t_1.r_comment, t_2.extra, t_2.price, t_2.channel) SELECT TIMESTAMP '2022-09-03 22:43:39' AS col_0, ((INT '235086829') + ((BIGINT '988') + ((SMALLINT '34') % (BIGINT '691')))) AS col_1, ((INT '-2147483648')) AS col_2 FROM with_0;
CREATE MATERIALIZED VIEW m3 AS WITH with_0 AS (SELECT (INT '252') AS col_0, t_1.p_type AS col_1 FROM part AS t_1 GROUP BY t_1.p_partkey, t_1.p_size, t_1.p_type) SELECT TIMESTAMP '2022-09-03 22:43:39' AS col_0 FROM with_0 WHERE true;
CREATE MATERIALIZED VIEW m4 AS SELECT t_0.col_1 AS col_0, t_0.col_2 AS col_1 FROM m2 AS t_0 WHERE true GROUP BY t_0.col_1, t_0.col_2 HAVING true;
CREATE MATERIALIZED VIEW m5 AS SELECT (to_char(TIMESTAMP '2022-08-27 23:43:40', (substr(hop_0.channel, (INT '267'), ((SMALLINT '520') / ((INT '168'))))))) AS col_0, 'XT21EdPRft' AS col_1, string_agg(hop_0.url, hop_0.url) AS col_2 FROM hop(bid, bid.date_time, INTERVAL '78301', INTERVAL '1409418') AS hop_0 WHERE false GROUP BY hop_0.channel HAVING (false);
CREATE MATERIALIZED VIEW m6 AS SELECT (FLOAT '818') AS col_0 FROM region AS t_0 FULL JOIN alltypes2 AS t_1 ON t_0.r_name = t_1.c9 AND t_1.c1 GROUP BY t_0.r_name, t_0.r_comment, t_1.c3, t_1.c16, t_1.c4;
CREATE MATERIALIZED VIEW m7 AS SELECT (INT '464') AS col_0, t_1.ps_availqty AS col_1, t_1.ps_suppkey AS col_2 FROM m0 AS t_0 JOIN partsupp AS t_1 ON t_0.col_0 = t_1.ps_comment GROUP BY t_1.ps_partkey, t_1.ps_suppkey, t_1.ps_availqty;
CREATE MATERIALIZED VIEW m9 AS SELECT count((coalesce(NULL, ((REAL '206')), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL))) FILTER(WHERE false) AS col_0, t_1.ps_partkey AS col_1 FROM bid AS t_0 JOIN partsupp AS t_1 ON t_0.url = t_1.ps_comment GROUP BY t_0.date_time, t_0.extra, t_0.price, t_1.ps_comment, t_0.bidder, t_1.ps_partkey HAVING false;

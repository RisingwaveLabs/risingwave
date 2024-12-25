CREATE SOURCE a (v1 int)
  WITH (
    connector = 'datagen',
    fields.v1.kind = 'random',
    datagen.rows.per.second = '100000'
  )
  FORMAT PLAIN
  ENCODE JSON;

CREATE MATERIALIZED VIEW mv
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv2
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv3
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv4
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv5
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv6
AS
SELECT
max(v1)
FROM a
GROUP BY v1;


CREATE MATERIALIZED VIEW mv7
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv8
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv9
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

CREATE MATERIALIZED VIEW mv10
AS
SELECT
max(v1)
FROM a
GROUP BY v1;

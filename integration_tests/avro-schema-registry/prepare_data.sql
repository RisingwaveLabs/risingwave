create table if not exists t (
    id bigint primary key,
    v1 boolean,
    -- v2 smallint, -- TODO: The conversion from smallint to avro int is not supported yet.
    v3 int,
    v4 bigint,
    v5 real,
    v6 double precision,
    v7 varchar,
    v8 bytea,
    v9 timestamptz,
    -- v10 timestamp, -- TODO: the mapping is not supported yet.
    v11 date,
    v12 time
);

INSERT INTO t (id, v1, v3, v4, v5, v6, v7, v8, v9, v11, v12) 
VALUES
    (1, TRUE, 100, 1000, 10.5, 20.75, 'Sample 1', E'\\x010203', '2023-12-25 12:00:00+00', '2023-12-25', '12:00:00'),
    (2, FALSE, 200, 2000, 20.5, 40.75, 'Sample 2', E'\\x040506', '2023-12-26 12:00:00+00', '2023-12-26', '13:00:00'),
    (3, TRUE, 300, 3000, 30.5, 60.75, 'Sample 3', E'\\x070809', '2023-12-27 12:00:00+00', '2023-12-27', '14:00:00'),
    (4, FALSE, 400, 4000, 40.5, 80.75, 'Sample 4', E'\\x0A0B0C', '2023-12-28 12:00:00+00', '2023-12-28', '15:00:00'),
    (5, TRUE, 500, 5000, 50.5, 100.75, 'Sample 5', E'\\x0D0E0F', '2023-12-29 12:00:00+00', '2023-12-29', '16:00:00');

FLUSH;

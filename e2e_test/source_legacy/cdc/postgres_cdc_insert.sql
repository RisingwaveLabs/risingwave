SELECT pg_current_wal_lsn();

INSERT INTO shipments
VALUES (default,10004,'Beijing','Shanghai',false);

INSERT INTO person VALUES (1203, '张三', 'kedmrpz@xiauh.com', '5536 1959 5460 2096', '北京');
INSERT INTO person VALUES (1204, '李四', 'egpemle@lrhcg.com', '0052 8113 1582 4430', '上海');

insert into abs.t1 values (2, 2.2, 'bbb', '1234.5431');

SELECT pg_current_wal_lsn();
select * from pg_publication_tables where pubname='rw_publication';
select * from public.person order by id;

INSERT INTO postgres_all_types VALUES ( True, 0, 0, 0, 0, 0, 0, '', '\x00', '0001-01-01', '00:00:00', '2001-01-01 00:00:00'::timestamp, '2001-01-01 00:00:00-8'::timestamptz, interval '0 second', '{}', null, 'sad', array[]::boolean[], array[]::smallint[], array[]::integer[], array[]::bigint[], array[]::decimal[], array[]::real[], array[]::double precision[], array[]::varchar[], array[]::bytea[], array[]::date[], array[]::time[], array[]::timestamp[], array[]::timestamptz[], array[]::interval[], array[]::jsonb[], array[]::uuid[], array[]::mood[]);
INSERT INTO postgres_all_types VALUES ( True, -32767, -2147483647, -9223372036854775807, -10.0, -9999.999999, -10000.0, 'd', '\x00', '0001-01-01', '00:00:00', '2001-01-01 00:00:00'::timestamp, '2001-01-01 00:00:00-8'::timestamptz, interval '0 second', '{}', 'bb488f9b-330d-4012-b849-12adeb49e57e', 'happy', array[False::boolean]::boolean[], array[-32767::smallint]::smallint[], array[-2147483647::integer]::integer[], array[-9223372036854775807::bigint]::bigint[], array[-10.0::decimal]::decimal[], array[-9999.999999::real]::real[], array[-10000.0::double precision]::double precision[], array[''::varchar]::varchar[], array['\x00'::bytea]::bytea[], array['0001-01-01'::date]::date[], array['00:00:00'::time]::time[], array['2001-01-01 00:00:00'::timestamp::timestamp]::timestamp[], array['2001-01-01 00:00:00-8'::timestamptz::timestamptz]::timestamptz[], array[interval '0 second'::interval]::interval[], array['{}'::jsonb]::jsonb[], '{bb488f9b-330d-4012-b849-12adeb49e57e}', '{happy,ok,NULL}');
INSERT INTO postgres_all_types VALUES ( True, 1, 123, 1234567890, 123.45, 123.45, 123.456, 'example', '\xDEADBEEF', '0024-01-01', '12:34:56', '2024-05-19 12:34:56', '2024-05-19 12:34:56+00', INTERVAL '1 day', '{"key": "value"}', '123e4567-e89b-12d3-a456-426614174000', 'happy', ARRAY[NULL, TRUE]::boolean[], ARRAY[NULL, 1::smallint], ARRAY[NULL, 123], ARRAY[NULL, 1234567890], ARRAY[NULL, 123.45::numeric], ARRAY[NULL, 123.45::real], ARRAY[NULL, 123.456], ARRAY[NULL, 'example'], ARRAY[NULL, '\xDEADBEEF'::bytea], ARRAY[NULL, '2024-05-19'::date], ARRAY[NULL, '12:34:56'::time], ARRAY[NULL, '2024-05-19 12:34:56'::timestamp], ARRAY[NULL, '2024-05-19 12:34:56+00'::timestamptz], ARRAY[NULL, INTERVAL '1 day'], ARRAY[NULL, '{"key": "value"}'::jsonb], ARRAY[NULL, '123e4567-e89b-12d3-a456-426614174000'::uuid], ARRAY[NULL, 'happy'::mood]);
INSERT INTO postgres_all_types VALUES ( True, NULL, NULL, 1, NULL, NULL, NULL, NULL, NULL, '0024-05-19', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

insert into numeric_table values(102, 57896044618658097711785492504343953926634992332820282019728792003956564819967);
--- 2^255
insert into numeric_table values(103, 57896044618658097711785492504343953926634992332820282019728792003956564819968);
--- 2^256
insert into numeric_table values(104, 115792089237316195423570985008687907853269984665640564039457584007913129639936);
insert into numeric_table values(105, 115792089237316195423570985008687907853269984665640564039457584007913129639936.555555);
insert into numeric_table values(106, 'NaN'::numeric);
insert into numeric_table values(107, 'Infinity'::numeric);

INSERT INTO enum_table VALUES (3, 'sad');
--- to avoid escaping issues of psql -c "", we insert this row here and check the result in check_new_rows.slt
INSERT INTO list_with_null VALUES (3, '{NULL,-3,-4}', '{NULL,nan,-inf}', '{NULL,nan,-inf}', '{NULL,nan,-inf}', '{NULL,sad,ok}', '{NULL,471acecf-a4b4-4ed3-a211-7fb2291f159f,9bc35adf-fb11-4130-944c-e7eadb96b829}', '{NULL,\\x99,\\xAA}');
INSERT INTO list_with_null VALUES (4, '{-4,-5,-6}', '{NULL,-99999999999999999.9999}', '{NULL,-99999999999999999.9999}', '{NULL,-99999999999999999.9999}', '{NULL,sad,ok}', '{b2e4636d-fa03-4ad4-bf16-029a79dca3e2}', '{\\x88,\\x99,\\xAA}');
INSERT INTO list_with_null VALUES (6, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO partitioned_timestamp_table (c_int, c_boolean, c_timestamp) VALUES
(11, true, '2023-02-01 10:30:00'),
(12, true, '2023-05-15 11:45:00'),
(13, true, '2023-11-03 12:15:00'),
(14, true, '2024-01-04 13:00:00'),
(15, true, '2024-03-05 09:30:00'),
(16, true, '2024-06-06 14:20:00'),
(17, true, '2024-09-07 16:45:00'),
(18, true, '2025-01-08 18:30:00'),
(19, true, '2025-07-09 07:10:00');

INSERT INTO
  json_table
VALUES
  (11, '{"id": 1, "name": "alice"}', '{"id": 1, "name": "alice"}', ARRAY['{"id": 1, "name": "alice"}'::json], ARRAY['{"id": 1, "name": "alice"}'::jsonb]),
  (12, '{}', '{}', ARRAY['{}'::json], ARRAY['{}'::jsonb]),
  (13, '{}', '{}', array[]::json[], array[]::jsonb[]),
  (14, NULL, NULL, NULL, NULL);

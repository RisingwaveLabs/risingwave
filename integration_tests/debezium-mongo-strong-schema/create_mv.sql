CREATE MATERIALIZED VIEW normalized_users AS
SELECT
    name as name,
    email as email,
    address as address
FROM
    users;

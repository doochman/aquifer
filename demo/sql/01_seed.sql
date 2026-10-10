-- Demo dataset "Acme Outdoor": a small, fully synthetic web shop (no real data, nothing downloaded).
-- Deterministic: u(key) hashes a key to [0, 1), so every run produces the same shape
-- relative to today's date. Run by `./lakehouse demo`; safe to run repeatedly.

CREATE SCHEMA IF NOT EXISTS iceberg.shop WITH (location = 's3://warehouse/shop/');

CREATE OR REPLACE TABLE iceberg.shop.products
WITH (format = 'PARQUET') AS
WITH FUNCTION u(k varchar) RETURNS double
  RETURN CAST(bitwise_and(from_big_endian_64(xxhash64(to_utf8(k))), 1048575) AS double) / 1048576
SELECT
  id AS product_id,
  category || ' ' || element_at(ARRAY['Classic', 'Pro', 'Lite', 'Trail', 'Summit', 'Core', 'Max'], 1 + CAST(floor(u('pn' || CAST(id AS varchar)) * 7) AS integer)) AS product_name,
  category,
  CAST(round(9 + u('pp' || CAST(id AS varchar)) * 111, 2) AS decimal(10, 2)) AS unit_price
FROM (
  SELECT id, element_at(ARRAY['Hiking', 'Camping', 'Cycling', 'Running', 'Climbing', 'Winter'], 1 + CAST(mod(id, 6) AS integer)) AS category
  FROM UNNEST(sequence(1, 48)) AS t(id)
);

CREATE OR REPLACE TABLE iceberg.shop.customers
WITH (format = 'PARQUET') AS
WITH FUNCTION u(k varchar) RETURNS double
  RETURN CAST(bitwise_and(from_big_endian_64(xxhash64(to_utf8(k))), 1048575) AS double) / 1048576
SELECT
  id AS customer_id,
  date_add('day', -CAST(floor(power(u('sd' || CAST(id AS varchar)), 0.8) * 540) AS integer), current_date) AS signup_date,
  element_at(ARRAY['DE', 'DE', 'DE', 'FR', 'FR', 'NL', 'ES', 'IT', 'PL', 'SE', 'GB'], 1 + CAST(floor(u('co' || CAST(id AS varchar)) * 11) AS integer)) AS country,
  element_at(ARRAY['organic', 'organic', 'paid_search', 'paid_search', 'social', 'email', 'referral'], 1 + CAST(floor(u('ch' || CAST(id AS varchar)) * 7) AS integer)) AS acquisition_channel,
  u('mk' || CAST(id AS varchar)) < 0.62 AS marketing_opt_in
FROM UNNEST(sequence(1, 5000)) AS t(id);

CREATE OR REPLACE TABLE iceberg.shop.orders
WITH (format = 'PARQUET') AS
WITH FUNCTION u(k varchar) RETURNS double
  RETURN CAST(bitwise_and(from_big_endian_64(xxhash64(to_utf8(k))), 1048575) AS double) / 1048576
SELECT
  g.order_id,
  g.customer_id,
  date_add('day', CAST(floor(u('od' || CAST(g.order_id AS varchar)) * (date_diff('day', c.signup_date, current_date) + 1)) AS integer), c.signup_date) AS order_date,
  g.product_id,
  g.quantity,
  g.discount_pct,
  CAST(round(g.quantity * p.unit_price * (1 - g.discount_pct), 2) AS decimal(12, 2)) AS amount,
  g.status
FROM (
  SELECT
    id AS order_id,
    1 + CAST(floor(power(u('oc' || CAST(id AS varchar)), 1.7) * 5000) AS bigint) AS customer_id,
    1 + CAST(floor(u('op' || CAST(id AS varchar)) * 48) AS bigint) AS product_id,
    1 + CAST(floor(u('q1' || CAST(id AS varchar)) * u('q2' || CAST(id AS varchar)) * 4) AS integer) AS quantity,
    CASE WHEN u('dc' || CAST(id AS varchar)) < 0.25 THEN 0.10 WHEN u('dc' || CAST(id AS varchar)) < 0.30 THEN 0.20 ELSE 0.00 END AS discount_pct,
    CASE WHEN u('st' || CAST(id AS varchar)) < 0.05 THEN 'refunded' WHEN u('st' || CAST(id AS varchar)) < 0.07 THEN 'cancelled' ELSE 'completed' END AS status
  -- sequence() is capped at 10,000 entries, so 3 x 10,000 gives 30,000 orders.
  FROM (SELECT i * 10000 + j AS id FROM UNNEST(sequence(0, 2)) AS a(i) CROSS JOIN UNNEST(sequence(1, 10000)) AS b(j)) AS t
) AS g
JOIN iceberg.shop.customers AS c ON c.customer_id = g.customer_id
JOIN iceberg.shop.products AS p ON p.product_id = g.product_id;

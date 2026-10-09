CREATE OR REPLACE TABLE users AS FROM read_csv(getvariable('bucket') || '/bronze/users.csv');
CREATE OR REPLACE TABLE orders AS FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');

-- 1. Average number of orders per user and average quantity per order
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- 2. First and last order of each user, and days between them
SELECT
  u.username,
  min(o.date)::DATE AS first_order,
  max(o.date)::DATE AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY days_between DESC
LIMIT 10;

-- 3. Hour of the day with the highest quantity sold
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 3;

-- 4. Month-over-month variation per product
WITH monthly AS (
  SELECT product, strftime(date, '%Y-%m') AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY ALL
)
SELECT
  product, month, quantity,
  round(100.0 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

-- 5. Users who ordered every product at least once
SELECT count(*) AS users_with_all_products
FROM (
  SELECT user_uuid
  FROM orders
  GROUP BY user_uuid
  HAVING count(DISTINCT product) = (SELECT count(DISTINCT product) FROM orders)
);

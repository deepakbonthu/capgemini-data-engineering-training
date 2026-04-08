-- Phase 2 SQL
-- 1 Total amount per customer
SELECT customer_id, SUM(amount)
FROM orders
GROUP BY customer_id;

-- 2 Top 3 customers
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
ORDER BY total DESC
LIMIT 3;

-- 3 Customers with no orders
SELECT c.customer_id
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;

-- 4 City-wise revenue
SELECT c.city, SUM(o.amount)
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.city;

-- 5 Average per customer
SELECT customer_id, AVG(amount)
FROM orders
GROUP BY customer_id;

-- 6 Customers with >1 order
SELECT customer_id
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- 7 Sort by total spend
SELECT customer_id, SUM(amount) AS total
FROM orders
GROUP BY customer_id
ORDER BY total DESC;
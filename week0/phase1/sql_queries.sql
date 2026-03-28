SELECT * FROM customers;

SELECT * FROM customers WHERE city='Chennai';

SELECT * FROM customers WHERE age > 25;

SELECT customer_name, city FROM customers;

SELECT city, COUNT(*) FROM customers GROUP BY city;

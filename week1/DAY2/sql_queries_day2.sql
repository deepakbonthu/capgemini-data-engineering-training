CREATE TABLE Employee (
emp_id INT PRIMARY KEY,
emp_name VARCHAR(50),
department VARCHAR(50),
salary DECIMAL(10, 2),
joining_date DATE
);
INSERT INTO Employee (emp_id, emp_name, department, salary, joining_date) VALUES
(1, 'Karthik', 'HR', 60000.00, '2021-01-15'),
(2, 'Pratik', 'Finance', 70000.00, '2021-03-10'),
(3, 'Veer', 'HR', 55000.00, '2021-06-20'),
(4, 'Priya', 'Finance', 80000.00, '2022-01-05'),
(5, 'Ajay', 'Engineering', 75000.00, '2020-11-01'),
(6, 'Vijay', 'Engineering', 78000.00, '2019-05-22'),
(7, 'Veena', 'HR', 62000.00, '2023-03-12'),
(8, 'Meena', 'Marketing', 65000.00, '2022-08-18');

CREATE TABLE Sales (
sales_id INT PRIMARY KEY,
emp_id INT,
product VARCHAR(50),
amount DECIMAL(10, 2),
sale_date DATE
);

INSERT INTO Sales (sales_id, emp_id, product, amount, sale_date) VALUES
(1, 1, 'Laptop', 50000.00, '2023-01-15'),
(2, 2, 'Mobile', 30000.00, '2023-02-18'),
(3, 3, 'Tablet', 20000.00, '2023-02-25'),
(4, 4, 'Laptop', 45000.00, '2023-03-05'),
(5, 5, 'Mobile', 35000.00, '2023-03-12'),
(6, 6, 'Tablet', 25000.00, '2023-03-20'),
(7, 7, 'Laptop', 60000.00, '2023-04-01'),
(8, 8, 'Mobile', 40000.00, '2023-04-10');


-- Find the total salary for each department in the Employee table. 
SELECT SUM(salary) as total_salary, department 
FROM Employee 
GROUP BY department; -- Added semicolon here

-- Count employees per department
SELECT COUNT(emp_id) as emp_count, department 
FROM Employee 
GROUP BY department;

-- 3. Calculate the average salary of employees in each department.
SELECT AVG(salary) as avg_salary , department 
FROM Employee
GROUP BY department;

-- 4. Find the maximum salary in each department.
SELECT MAX(salary) ,department
FROM Employee
GROUP BY department;

-- 5. Find the minimum salary in each department.

SELECT MIN(salary) ,department
FROM Employee
GROUP BY department;

-- 6. Find the total salary for departments where the total salary exceeds 100,000.
SELECT SUM(salary) as total_salary ,department 
FROM Employee
GROUP BY department
HAVING total_salary > 100000 ;

-- 7. Count the number of employees in departments with more than 2 employees.
SELECT COUNT(emp_id) as total_employees ,department 
FROM Employee
GROUP BY department
HAVING total_employees > 2 ;

-- 8. Calculate the average salary for employees who joined after 2021-01-01, grouped bydepartment.
 
 SELECT AVG(salary) as avg_sal , department 
 FROM Employee
 WHERE joining_date > 2021-01-01
 GROUP BY department;

-- 9. Find the departments where the maximum salary is greater than 75,000.
SELECT MAX(salary) as max_salary ,department 
FROM Employee
GROUP BY department
HAVING max_salary > 75000 ;

-- 10. List the departments where the total salary is less than 150,000.
SELECT SUM(salary) as total_salary ,department 
FROM Employee
GROUP BY department
HAVING total_salary < 150000 ;
    
    -- 11. Find the total number of employees grouped by department, but only include departments with more than 1 employee.
SELECT COUNT(emp_id) as total_emp , department
FROM Employee
GROUP BY department
HAVING total_emp > 1 ;

-- 12. Calculate the total salary of each department and show only those where the total exceeds 125,000
SELECT SUM(salary) as total_salary , department
FROM Employee
GROUP BY department
HAVING total_salary > 125000 ;

-- 13. Count the number of employees in each department, but include only departments with more than 2 employees.
SELECT COUNT(emp_id) as total_emp , department
FROM Employee
GROUP BY department
HAVING total_emp > 2 ;

-- 14. Find the average salary in each department where the average salary is above 60,000.

SELECT AVG(salary) as avg_salary , department
FROM Employee
GROUP BY department
HAVING avg_salary > 60000 ;

-- 15. Show departments where the sum of salaries is between 100,000 and 200,000.
SELECT SUM(salary) as total_salary , department
FROM Employee
GROUP BY department
HAVING total_salary BETWEEN 100000 AND 200000 ;

-- 16. Find the total sales amount for each employee.
SELECT Employee.emp_name, SUM(Sales.amount) as total_sales
FROM Sales
LEFT JOIN Employee ON Employee.emp_id = Sales.emp_id
GROUP BY Employee.emp_name;

-- 17. List the number of sales made by each employee.
SELECT Employee.emp_name, COUNT(Sales.sales_id) as los
FROM Sales
LEFT JOIN Employee ON Employee.emp_id = Sales.emp_id
GROUP BY Employee.emp_name;

-- 18. Find the total sales amount grouped by product.
SELECT Sales.product ,SUM(Sales.amount) as total_sales
FROM Sales
LEFT JOIN Employee ON Employee.emp_id = Sales.emp_id
GROUP BY Sales.product ;

-- 19. Calculate the average sales amount grouped by product.
SELECT Sales.product ,AVG(Sales.amount) as total_sales
FROM Sales
LEFT JOIN Employee ON Employee.emp_id = Sales.emp_id
GROUP BY Sales.product ;

-- 20. Find employees who have made more than 2 sales, grouped by their names.
SELECT Employee.emp_name,Count(Sales.sales_id) as sc
FROM Sales
LEFT JOIN Employee on Employee.emp_id=Sales.emp_id
GROUP BY Employee.emp_name
HAVING sc >= 2;

-- 21. Calculate the total salary and total sales amount for each employee.
SELECT SUM(Employee.salary) as total_salary,SUM(Sales.amount) as total_sales
FROM Sales
LEFT JOIN Employee
on Employee.emp_id=Sales.sales_id ;

-- 22. Count the number of unique products sold by each employee.
SELECT Employee.emp_name, COUNT(DISTINCT Sales.product) as unique_product_count
FROM Sales
LEFT JOIN Employee ON Employee.emp_id = Sales.emp_id
GROUP BY Employee.emp_name;

-- 23. Find the highest sales amount made by each employee.
SELECT Employee.emp_NAME,MAX(Sales.amount) as maxi
FROM Sales
LEFT JOIN Employee
on Employee.emp_id=Sales.emp_id
GROUP BY Employee.emp_id ;

-- 24. Calculate the total sales amount grouped by product and filtered by products where the total exceeds 50,000.
SELECT SUM(amount) as total_sales , product
FROM Sales
GROUP BY product
HAVING total_sales > 50000 ;

-- 25. Find the departments with the highest average sales amount.
SELECT AVG(Sales.amount) as avg_amt, Employee.department 
FROM Sales 
LEFT JOIN Employee on Employee.emp_id = Sales.emp_id 
GROUP BY Employee.department
ORDER BY avg_amt DESC
LIMIT 1;

-- 26. Find the department with the highest total sales amount.

SELECT SUM(Sales.amount) as avg_amt, Employee.department 
FROM Sales 
LEFT JOIN Employee on Employee.emp_id = Sales.emp_id 
GROUP BY Employee.department
ORDER BY avg_amt DESC
LIMIT 1;

-- 27. Show the top 3 employees with the highest total sales amount, grouped by employee names.
SELECT SUM(Sales.amount) as sum_amt, Employee.emp_name
FROM Sales 
LEFT JOIN Employee 
on Employee.emp_id = Sales.emp_id 
GROUP BY Employee.emp_name
ORDER BY sum_amt DESC
LIMIT 3;

-- 28. Calculate the total number of employees and the average salary, grouped by the year of joining.
SELECT COUNT(emp_id) as emp_count,AVG(Salary)  AS avg_sal
FROM Employee
GROUP BY joining_date ;

-- 29. Find the total sales amount for each department (using a join between Employee and Sales).
SELECT SUM(Sales.amount) as total_sales , Employee.department  
FROM Sales
LEFT JOIN Employee
ON Employee.emp_id=Sales.sales_id
GROUP BY Employee.department ;

-- 30. Show employees who have not made any sales, grouped by their department
SELECT Employee.emp_name, Employee.department, COUNT(Sales.sales_id) as sc
FROM Employee
LEFT JOIN Sales ON Employee.emp_id = Sales.emp_id
GROUP BY Employee.department, Employee.emp_name
HAVING sc = 0;


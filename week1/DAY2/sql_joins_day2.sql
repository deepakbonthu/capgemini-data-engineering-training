CREATE TABLE employees (
    emp_id INT PRIMARY KEY,
    emp_name VARCHAR(50),
    manager_id INT,
    dept_id INT
);

INSERT INTO employees (emp_id, emp_name, manager_id, dept_id) VALUES
(1, 'Karthik', NULL, 1),
(2, 'Ajay', 1, 1),
(3, 'Vijay', 1, 2),
(4, 'Vinay', 2, 2),
(5, 'Meena', 3, 3),
(6, 'Veer', NULL, 4),
(7, 'Keerthi', 4, 5),
(8, 'Priya', 4, 5);

CREATE TABLE departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50)
);

INSERT INTO departments (dept_id, dept_name) VALUES
(1, 'HR'),
(2, 'IT'),
(3, 'Finance'),
(4, 'Marketing'),
(5, 'Sales');

CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(50),
    emp_id INT
);

INSERT INTO projects (project_id, project_name, emp_id) VALUES
(1, 'Project A', 1),
(2, 'Project B', 2),
(3, 'Project C', 3),
(4, 'Project D', 4),
(5, 'Project E', 5);



    -- 1. Retrieve the names of employees and their corresponding managers from the "employees" table, ensuring that even employees without managers are included.
 SELECT 
    e.emp_name AS Employee, 
    m.emp_name AS Manager
FROM employees e
 JOIN employees m ON e.manager_id = m.emp_id;

-- 2. Display all employees and their corresponding departments from the "employees" and "departments" tables, showing employees even if they don't belong to any department
 SELECT employees.emp_name,departments.dept_name 
 FROM departments
 JOIN employees
 on employees.dept_id=departments.dept_id ;

-- 3. List the names of employees who report to a manager, along with their manager's name, from the "employees" table
 SELECT 
    e.emp_name AS Employee, 
    m.emp_name AS Manager
FROM employees e
LEFT JOIN employees m ON e.manager_id = m.emp_id;


-- 4. Find the total salary paid to each employee and their respective department, including departments with no employees.



-- 5. Display a list of employees who do not belong to any department, even if the department data is missing.
SELECT e.emp_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
WHERE d.dept_id IS NULL;


-- 6. Fetch the names of employees and the projects they are assigned to. For employees who are not assigned any projects, show NULL for the project.
SELECT e.emp_name,p.project_name
FROM employees e
LEFT JOIN projects p
ON e.emp_id=p.emp_id ; 

-- 7. List all employees who have completed at least one project, showing their names and the project names.
SELECT e.emp_name,p.project_name
FROM employees e
 JOIN projects p
on e.emp_id=p.emp_id ;

-- 8. Show the names of employees and their projects, ensuring that no project is omitted even if an employee is not assigned to it.
 SELECT e.emp_name,p.project_name 
 FROM projects p
 LEFT JOIN employees e
 ON e.emp_id=p.emp_id ;
 
 -- 9. Find all employees and their corresponding salaries, and display NULL for salary if there is no salary record for the employee.


-- 10. Retrieve the names of employees and their corresponding department names, including employees who are not in any department.
SELECT e.emp_name,d.dept_name
FROM employees e
LEFT JOIN departments d
ON e.dept_id=d.dept_id ;

-- 11. Find the names of all departments and employees, ensuring that departments with no employees are included.
SELECT d.dept_name,e.emp_name 
FROM departments d
LEFT JOIN employees e
ON e.dept_id=d.dept_id ;

-- 12. List all employees with their contact information, including employees without contact records.


-- 13. Show the names of employees and their department names, including employees not assignedto any department and departments without employees.
SELECT e.emp_name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id

UNION

SELECT e.emp_name, d.dept_name
FROM employees e
RIGHT JOIN departments d ON e.dept_id = d.dept_id;

-- 14. Find employees who have not completed any project, along with the project details where applicable
-- 14. List names of employees and projects, including those with no assignments on either side.
SELECT e.emp_name, p.project_name
FROM employees e
LEFT JOIN projects p ON e.emp_id = p.emp_id

UNION

SELECT e.emp_name, p.project_name
FROM employees e
RIGHT JOIN projects p ON e.emp_id = p.emp_id;

-- 15. Retrieve the names of employees and their project names, 
-- including employees who are not working on any project.
SELECT e.emp_name, p.project_name
FROM employees e
LEFT JOIN projects p ON e.emp_id = p.emp_id;


-- 16. List all projects and the employees assigned to them, even for projects that have no employees.
SELECT p.project_name,e.emp_name
FROM projects p
LEFT JOIN employees e
ON e.emp_id=p.emp_id ;


-- 17. Show the names of all employees who have both a manager and at least one project, listing the manager's name as well.
SELECT 
    e.emp_name AS Employee, 
    m.emp_name AS Manager,
    p.project_name
FROM employees e
JOIN employees m ON e.manager_id = m.emp_id
JOIN projects p ON e.emp_id = p.emp_id;

-- 18. List the names of employees and the corresponding department names, but exclude those employees who don't belong to a department.
 SELECT e.emp_name,d.dept_name
 FROM employees e
 JOIN departments d
 ON e.dept_id=d.dept_id ;
 
 -- 19. Display employees who belong to multiple departments, showing the employee's name and the department names.
SELECT 
    e.emp_name, 
    GROUP_CONCAT(d.dept_name SEPARATOR ', ') AS department_list
FROM employees e
JOIN departments d ON e.dept_id = d.dept_id
GROUP BY e.emp_name
HAVING COUNT(DISTINCT e.dept_id) > 1;

-- 20. List the names of all departments and employees, ensuring that even if a department has noemployees, it is included in the result.
SELECT 
    d.dept_name, 
    COALESCE(e.emp_name, 'No Employees Assigned') AS employee_name
FROM departments d
LEFT JOIN employees e ON d.dept_id = e.dept_id;


-- 21. Retrieve employees who have worked on at least one project and do not belong to a department, listing their name and project details.
SELECT e.emp_name,p.project_name
FROM employees e
JOIN projects p
ON e.emp_id=p.emp_id 
WHERE e.dept_id IS NULL ;

-- 22. Find the total number of employees who belong to a department, ensuring the departments with no employees are still included.
SELECT d.dept_name,COUNT(e.emp_id) as total_employees
FROM departments d
LEFT JOIN employees e
ON e.dept_id=d.dept_id
GROUP BY d.dept_name ;


-- 23. Show the employees and their managers, displaying only those employees who report to a manager, excluding employees without managers
SELECT e.emp_name as employee,m.emp_name as manager
FROM employees e
JOIN employees  m ON e.manager_id = m.emp_id;


-- 24. Display all employee names along with their corresponding managers' names, but include employees who do not have managers.
SELECT e.emp_name as employee,m.emp_name as manager
FROM employees e
JOIN employees  m ON e.manager_id = m.emp_id;

-- 25. Find the names of departments and the number of employees in each department, including departments that have no employees.
SELECT d.dept_name,COUNT(e.emp_id) as no_of_emps
FROM departments d
LEFT JOIN employees e
on d.dept_id=e.dept_id
GROUP BY d.dept_name ;

-- 26. List all employees and the departments they belong to, ensuring that departments with no employees are also listed.
SELECT e.emp_name,d.dept_name
FROM departments d
LEFT JOIN employees e
on d.dept_id=e.dept_id ;

-- 28. Retrieve the names of employees and their project assignments, including employees who are not assigned to any projects.

SELECT e.emp_name, p.project_name
FROM employees e
LEFT JOIN projects p ON e.emp_id = p.emp_id;

-- 29. List the names of all employees and their respective department and project assignments,including employees who are not assigned to a project or department.
SELECT 
    e.emp_name, 
    d.dept_name, 
    p.project_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id
LEFT JOIN projects p ON e.emp_id = p.emp_id;


-- 30. Display the names of employees who belong to at least one department, with the department name listed, but include employees without a department as well

SELECT 
    e.emp_name, 
    d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.dept_id;
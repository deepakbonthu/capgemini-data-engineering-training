# Phase 6 – Spark Playground Exit Sprint (Advanced Practice Lab)

## 1. Objective
The objective of this phase is to build strong fluency and confidence in **PySpark** by practicing joins, window functions, date operations, and executing a complete data pipeline before transitioning to Databricks.

---

## 2. Problem Statement
This phase focuses on hands-on practice using a starter dataset.

### Key Tasks:
- Practice different types of joins  
- Apply window functions for ranking and analysis  
- Perform date-based transformations and aggregations  
- Execute a complete data pipeline within a time limit  
- Validate results at each step  

---

## 3. Approach

### Data Loading & Inspection
- Loaded dataset using **PySpark**  
- Inspected data using `show()` and `printSchema()`  

### Join Operations
- Applied **inner join** to retrieve matching records  
- Used **left join** to identify missing values  
- Applied **left anti join** to detect invalid foreign keys  
- Validated joins using row count comparisons  

### Window Functions
- Ranked top customers within each city  
- Calculated running total of sales  
- Ranked customers based on total spend  
- Used `lag()` function to track previous transactions  

### Date Analysis
- Extracted month from date column  
- Calculated monthly sales aggregation  
- Computed differences between dates  
- Analyzed trends based on monthly data  

### Pipeline Execution
- Loaded and cleaned data  
- Filtered invalid records  
- Validated referential integrity  
- Joined datasets  
- Applied aggregations and window functions  
- Saved final output within the time limit  

---

## 4. Key Transformations Used
- `join()` → Combining datasets  
- `groupBy()` → Aggregations  
- `agg()` → Sum and count calculations  
- `withColumn()` → Creating new columns  
- `filter()` → Removing invalid data  
- Window Functions → Ranking and running totals  
- `lag()` → Accessing previous records  
- Date Functions → Extracting and transforming date values  

---

## 5. Outputs & Results
The following outputs were generated:
- Valid joined dataset  
- Top customers per city  
- Running total of sales  
- Monthly sales aggregation  
- Trend analysis results  
- Final pipeline output stored as **`phase6_final_detailed_v2`**  

---

## 6. Data Engineering Considerations
- Validated joins using row counts and null checks  
- Ensured referential integrity between datasets  
- Handled missing and invalid data before processing  
- Maintained correct data types for date operations  
- Verified intermediate results at each stage  

---

## 7. Challenges Faced
- Understanding different join types and their behavior  
- Implementing window functions correctly  
- Working with date transformations  
- Managing time during pipeline execution  
- Debugging errors efficiently  

---

## 8. Key Learnings
- Improved understanding of PySpark transformations  
- Gained confidence in joins and window functions  
- Learned how to debug and validate data  
- Increased speed and accuracy in problem-solving  
- Developed ability to build complete pipelines independently  

---

## 9. Reflection Questions
- Which task took the most time?  
- What mistakes were made during implementation?  
- How were issues debugged?  
- Can the pipeline be built independently?  
- What areas need improvement?  

---

## 10. Project Structure
├── phase6_problem_statement/     # Contains detailed task description
├── phase6_dirty_starter/        # Raw dataset and initial practice files
├── solution/                    # PySpark implementation scripts
├── OUTPUTS/                     # Results, outputs, and screenshots
└── README.md                    # Project documentation

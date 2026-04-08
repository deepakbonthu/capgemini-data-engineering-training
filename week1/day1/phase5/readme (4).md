# Phase 5 – Databricks & Olist End-to-End Data Engineering Pipeline

## 1. Objective
In this phase, we worked with a real-world dataset to build a complete data engineering pipeline using **PySpark in Databricks**. The objective was to perform data ingestion, transformation, analysis, and generate meaningful business insights by integrating multiple datasets.

---

## 2. Problem Statement
We used the **Olist E-commerce dataset**, which includes multiple tables such as:
- Customers  
- Orders  
- Products  
- Order Items  
- Payments  

### Key Tasks:
- Load multiple datasets from CSV files  
- Join datasets to establish relationships  
- Perform aggregations and calculations  
- Apply window functions for advanced analytics  
- Create customer segmentation  
- Build a final reporting dataset  

---

## 3. Approach

### Data Ingestion
- Loaded all datasets using **PySpark** from Databricks storage  
- Verified data using `show()` and schema inspection  

### Data Integration
- Joined **customers, orders, and payments** for customer-level analysis  
- Joined **order items and products** for product-level insights  

### Transformations
- Calculated **total spend per customer**  
- Computed **daily sales**  
- Aggregated **product sales by category**  

### Advanced Analytics (Window Functions)
- Ranked customers within each city  
- Calculated running total of sales  
- Identified top products per category  

### Customer Segmentation
- Segmented customers into:
  - **Gold**
  - **Silver**
  - **Bronze**  
  based on total spending  

### Final Dataset
- Created a unified reporting dataset containing:
  - Customer details  
  - City  
  - Total spend  
  - Segment  
  - Order count  

---

## 4. Key Transformations Used
- `groupBy()` → Aggregations  
- `agg()` → Sum, count calculations  
- `join()` → Combine datasets  
- `withColumn()` → Create new columns  
- `when()` → Conditional logic  
- Window Functions → Advanced analytics  
- `rank()` / `dense_rank()` → Ranking  
- `to_date()` → Date conversion  

---

## 5. Outputs & Results
The pipeline produced the following insights:
- Top 3 customers in each city based on spending  
- Daily sales with running totals  
- Top-selling products by category  
- Customer Lifetime Value (CLV)  
- Customer segmentation (Gold, Silver, Bronze)  
- Final reporting dataset with consolidated insights  

---

## 6. Data Engineering Considerations
- Ensured accurate joins to maintain relationships  
- Prevented duplication during aggregations  
- Applied window functions efficiently  
- Maintained consistent column naming and data types  
- Validated intermediate outputs for correctness  

---

## 7. Challenges Faced
- Handling large and multiple datasets  
- Understanding relationships between tables  
- Implementing window functions correctly  
- Avoiding duplicate data during joins  
- Ensuring accurate aggregation results  

---

## 8. Key Learnings
- End-to-end data pipeline development using PySpark  
- Hands-on experience with real-world datasets  
- Performing complex joins and aggregations  
- Using window functions for advanced analytics  
- Building final reporting datasets from multiple sources  

---
## 9. Project Structure
├── solution/                  # PySpark implementation
├── phase5_problem_statement/ # Problem description
├── OUTPUTS/                  # Results and screenshots
└── README.md                 # Project documentation

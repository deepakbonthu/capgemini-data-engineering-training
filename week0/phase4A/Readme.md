# PySpark Customer Segmentation Project – Phase 4A

## Project Overview
This project implements customer segmentation using PySpark by applying different bucketing and ranking techniques. The objective is to transform continuous customer transaction data into meaningful categories that help analyze customer behavior and business performance.

## Objective
The main objective of Phase 4A is to apply bucketing and segmentation techniques in PySpark to categorize customers based on:

- Spending behavior  
- Purchase frequency  
- Relative performance ranking  

This helps organizations understand customer value and improve decision-making.

## Problem Description
Customer and sales datasets are analyzed to perform the following tasks:

- Calculate total spending of each customer  
- Determine customer order frequency  
- Apply multiple segmentation techniques  
- Compare segmentation approaches and their outcomes  

## Project Workflow

### Data Preparation
- Loaded datasets from CSV files  
- Enabled schema inference using inferSchema=True  
- Verified dataset structure using show() and printSchema()  
- Joined customer and sales datasets using customer_id  
- Created a new column full_name using concat_ws()  

### Data Aggregation
Customer metrics were calculated using PySpark aggregations:

- Total Spend calculated using sum()  
- Total Orders calculated using count()  

Functions used:
- groupBy()  
- agg()  
- sum()  
- count()  

### Segmentation Techniques

#### Conditional Segmentation (Rule-Based)
Used predefined thresholds with when() conditions.

Example categories:
- High Value Customers  
- Medium Value Customers  
- Low Value Customers  

#### Quantile-Based Segmentation
Used approxQuantile() to create data-driven segmentation based on spending distribution.

Advantages:
- Dynamic thresholds  
- Balanced segmentation  

#### Bucketizer Segmentation (MLlib)
Used Spark MLlib Bucketizer to convert continuous spending values into ranges or bins.

Purpose:
- Range-based grouping  
- Machine learning friendly segmentation  

#### Window-Based Ranking
Used window functions with percent_rank() to rank customers relative to others.

Purpose:
- Relative customer comparison  
- Performance ranking  

### Analysis
- Grouped customers by segment category  
- Compared distributions across segmentation methods  
- Evaluated differences between rule-based and dynamic segmentation  

## Key PySpark Transformations

- join() for merging datasets  
- groupBy() for aggregation operations  
- agg() for computing metrics  
- withColumn() for feature creation  
- when() for conditional segmentation  
- approxQuantile() for data-driven thresholds  
- Bucketizer for range-based bucketing  
- percent_rank() for customer ranking  

## Results
- Generated customer segments using multiple methods  
- Identified spending patterns  
- Produced bucketized customer categories  
- Created ranking-based customer insights  
- Compared segmentation outcomes effectively  

## Data Engineering Considerations
- Ensured schema correctness using inferSchema  
- Prevented duplicate records during aggregation  
- Maintained consistent column naming  
- Selected segmentation methods based on data distribution  

## Challenges Faced
- Understanding differences between segmentation strategies  
- Implementing quantile-based segmentation correctly  
- Applying window functions efficiently  
- Comparing results from multiple approaches  

## Key Learnings
- Practical experience with PySpark segmentation  
- Understanding fixed versus dynamic segmentation  
- Usage of MLlib Bucketizer  
- Application of window functions for ranking  
- Improved analytical and data engineering skills  

## Project Structure
project-root/
│
├── solution/ # PySpark implementation
├── phase4a_problem_statement/ # Problem description
├── OUTPUTS/ # Generated output files
└── README.md # Documentation

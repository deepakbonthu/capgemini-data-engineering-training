# 📊 Student Submission Analysis

## 🚀 Overview

This project analyzes student submissions across multiple datasets to identify:

* Valid submissions
* Duplicate submissions
* Students who did not submit
* Invalid submissions

The analysis is performed using **Databricks SQL and PySpark**, focusing on real-world data cleaning, joins, and window functions.

---

## 📂 Dataset Description

The project uses **3 datasets**:

### 1️⃣ Master Table

* Contains student details
* Columns:

  * `Superset ID`
  * `College Domain MAILID`
  * `Personal Mail ID`

### 2️⃣ Task1_Responses

* Contains submission data (email-based)

### 3️⃣ Task1_File2

* Contains additional submissions (duplicates + invalid entries)

---

## ⚙️ Technologies Used

* Databricks
* PySpark (for data loading)
* SQL (for transformations & analysis)

---

## 🔄 Workflow / Steps

### 🔹 Step 1: Data Loading

Data is loaded using PySpark and converted into SQL temp views:

* `master_raw`
* `task1_responses_raw`
* `task1_file2_raw`

---

### 🔹 Step 2: Data Cleaning

* Emails are normalized using:

  * `LOWER()` → convert to lowercase
  * `TRIM()` → remove spaces

New cleaned columns:

* `college_email_clean`
* `personal_email_clean`
* `email_clean`

---

### 🔹 Step 3: Email Mapping

* Each student has 2 emails
* Created a unified mapping table:

```
student_id | email
```

This helps match submissions correctly.

---

### 🔹 Step 4: Mapping Submissions

* Used **LEFT JOIN** to map emails to student IDs
* Results:

  * Matched → valid student
  * Not matched → invalid submission

---

### 🔹 Step 5: Combine Data

* Combined both submission datasets using:

```
UNION ALL
```

* Keeps duplicates for further analysis

---

### 🔹 Step 6: Duplicate Detection

Used window function:

```
ROW_NUMBER() OVER (PARTITION BY student_id)
```

* `rn = 1` → first submission
* `rn > 1` → duplicate submissions

---

### 🔹 Step 7: Final Classification

Each student is categorized as:

| Condition            | Status           |
| -------------------- | ---------------- |
| No submission        | NOT_SUBMITTED    |
| First submission     | VALID_SUBMISSION |
| Multiple submissions | DUPLICATE        |

---

## 📊 Key Insights

* Identified students who did not submit
* Detected duplicate submissions
* Found invalid email entries
* Analyzed student behavior using SQL

---

## 🧠 Concepts Used

* Data Cleaning (LOWER, TRIM)
* Joins (LEFT JOIN)
* Set Operations (UNION ALL)
* Window Functions (ROW_NUMBER)
* Conditional Logic (CASE)

---

## 💡 Learning Outcomes

* Handling real-world messy data
* Performing multi-table joins
* Identifying duplicates using window functions
* Building end-to-end data analysis pipeline

---

## 🚀 Future Improvements

* Use timestamp for accurate duplicate detection
* Build dashboard for visualization
* Convert logic into PySpark pipeline
* Optimize queries for large datasets

---

## 👨‍💻 Author

**Deepak Bonthu**

---

## ⭐ Conclusion

This project demonstrates a complete data analysis workflow using Databricks, showcasing skills in SQL, data cleaning, and analytical thinking.

# Create Borrower Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Borrowers (
    Borrowers_id_sk BIGINT,
    Borrowers_id_bk BIGINT,
    Emp_title STRING,
    Employment_length STRING,
    Annual_income DECIMAL(12,2),
    Home_ownership STRING,
    State_code STRING,
    Total_account INT,
    Verification_status STRING,
    Application_type STRING,
    insert_date TIMESTAMP
)
STORED AS PARQUET;

CREATE TABLE IF NOT EXISTS Dim_Credit_Grade (
    Credit_grade_sk BIGINT,
    Grade STRING,
    Subgrade STRING,
    insert_date TIMESTAMP
)
STORED AS PARQUET;


# Create Loan Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Loan_Term (
    Loan_Term_sk BIGINT,
    Period STRING,
    Term_description STRING,
    insert_date TIMESTAMP
)
STORED AS PARQUET;


# Create Status Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Status (
    Status_id_sk BIGINT,
    Status_id BIGINT,
    Loan_status STRING,
    Loan_status_category STRING,
    insert_date TIMESTAMP
)
STORED AS PARQUET;


# Create Date Dimension Table
CREATE TABLE IF NOT EXISTS Dim_Date (
    Date_key BIGINT,
    Date STRING,
    Year INT,
    Month INT,
    Month_name STRING,
    Quarter INT,
    insert_date TIMESTAMP
)
STORED AS PARQUET;


# Create Fact Table
CREATE TABLE IF NOT EXISTS Fact_Loan (
    Loan_id_pk_sk BIGINT,
    Loan_id_bk BIGINT,
    Status_id_fk BIGINT,
    Borrowers_id_fk BIGINT,
    Date_key_issue BIGINT,
    Date_key_last_payment BIGINT,
    Date_key_next_payment BIGINT,
    Last_credit_pull_date BIGINT,
    Credit_grade_fk BIGINT,
    Loan_Term_fk BIGINT,
    Loan_amount INT,
    DTI DECIMAL(6,5),
    Installment DECIMAL(12,2),
    Interest_rate DECIMAL(6,5),
    Total_payment INT,
    Purpose STRING,
    insert_date TIMESTAMP
)
STORED AS PARQUET;


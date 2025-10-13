-- Create Database
-- CREATE DATABASE IF NOT EXISTS Loans_DWH
-- LOCATION '/dwh_financial_loans/external/Loans_DWH';

USE DEFAULT;

/* Create Borrower Dimension Table */
CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Borrowers (
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
STORED AS ORC
LOCATION '/dwh_financial_loans/external/Dim_Borrowers';

CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Credit_Grade (
    Credit_grade_sk BIGINT,
    Grade STRING,
    Sub_grade STRING,
    insert_date TIMESTAMP
)
STORED AS ORC
LOCATION '/dwh_financial_loans/external/Dim_Credit_Grade';


/* Create Loan Dimension Table */
CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Loan_Term (
    Loan_Term_sk BIGINT,
    Period STRING,
    insert_date TIMESTAMP,
    Term_description STRING
)
STORED AS ORC
LOCATION '/dwh_financial_loans/external/Dim_Loan_Term';


/* Create Status Dimension Table */
CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Status (
    loan_status STRING,
    status_id_sk BIGINT,
    loan_status_category STRING,
    insert_date TIMESTAMP
)
STORED AS ORC
LOCATION '/dwh_financial_loans/external/Dim_Status';


/* Create Date Dimension Table */
CREATE EXTERNAL TABLE IF NOT EXISTS Dim_Date (
    date_key BIGINT,
    `date` DATE,
    year INT,
    month INT,
    month_name STRING,
    quarter INT,
    insert_date TIMESTAMP
)
STORED AS ORC
LOCATION '/dwh_financial_loans/external/Dim_Date';


/* Create Fact Table*/
CREATE EXTERNAL TABLE IF NOT EXISTS Fact_Loan (
    Loan_id_pk_sk BIGINT,
    Loan_id_bk BIGINT,
    Status_id_fk BIGINT,
    Borrowers_id_fk BIGINT,
    Date_key_issue BIGINT,
    Date_key_last_payment BIGINT,
    Date_key_next_payment BIGINT,
    last_credit_pull_date BIGINT,
    Credit_grade_fk BIGINT,
    Loan_Term_fk BIGINT,
    Loan_amount DECIMAL(6,2),
    dti DECIMAL(6,5),
    Installment DECIMAL(10,2),
    Interest_rate DECIMAL(5,2),
    Total_payment DECIMAL(10,2),
    purpose STRING,
    insert_date TIMESTAMP
)
STORED AS ORC
LOCATION '/dwh_financial_loans/external/Fact_Loan';

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, date_format, regexp_replace, trim, monotonically_increasing_id, udf, when, concat_ws, lit, to_date
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType
from datetime import datetime, timedelta


spark = SparkSession.builder.appName("Transform_Loan_Data").enableHiveSupport().getOrCreate()
financial_df = spark.read.parquet("/staging_layer") 

## Convert Dates From string to Date valid format
def clean_and_overwrite(df, col_name):
    return df.withColumn(
        col_name,
        F.coalesce(
            F.to_date(F.col(col_name), "d/M/yyyy"),
            F.to_date(F.col(col_name), "dd-MM-yyyy") 
        )
    )

financial_df = (
    financial_df
    .transform(lambda df: clean_and_overwrite(df, "issue_date"))
    .transform(lambda df: clean_and_overwrite(df, "last_credit_pull_date"))
    .transform(lambda df: clean_and_overwrite(df, "last_payment_date"))
    .transform(lambda df: clean_and_overwrite(df, "next_payment_date"))
)

state_dict = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia"
}


# Create a UDF to map codes to full names
code_to_name_udf = udf(lambda code: state_dict.get(code, "Unknown"), StringType())

# Add full state name column
financial_df= financial_df.withColumn("address_state", code_to_name_udf(col("address_state")))

financial_df = financial_df.withColumn(
    "term",
    trim(regexp_replace(col("term"), " months?|months", ""))
)

financial_df= financial_df.fillna({"emp_title": "Unknown"})
    
## Borrowers Dimension Mapping FKs and BKs, Generating Surrogate Keys    
    
dim_borrowers = financial_df.select(
    col("member_id").alias("borrowers_id_bk"),   # Business Key
    col("emp_title").alias("employment_title"),
    col("emp_length").alias("employment_length"),
    col("annual_income"),
    col("home_ownership"),
    col("address_state").alias("state_code"),
    col("total_acc").alias("total_account"),
    col("verification_status"),
    col("application_type")
).dropDuplicates(["borrowers_id_bk"]) \
 .withColumn("borrowers_id_sk", monotonically_increasing_id() + 1)

dim_borrowers = dim_borrowers.select(
    "borrowers_id_sk",
    "borrowers_id_bk",
    "employment_title",
    "employment_length",
    "annual_income",
    "home_ownership",
    "state_code",
    "total_account",
    "verification_status",
    "application_type"  
)

## Status Dimension Mapping FKs and BKs, Generating Surrogate Keys

dim_status = financial_df.select(
    col("loan_status")
).dropDuplicates(["loan_status"]) \
 .withColumn("status_id_sk", monotonically_increasing_id() + 1)

# Add loan_status_category column
dim_status = dim_status.withColumn(
    "loan_status_category",
    when(col("loan_status").isin("Fully Paid", "Current"), "Good")
    .otherwise("Bad")
)

dim_status = dim_status.select(
    "loan_status",
    "status_id_sk",
    "loan_status_category"
)

## Credit Grade Dimension Mapping FKs and BKs, Generating Surrogate Keys
dim_credit_grade = financial_df.select(
    col("grade"),
    col("sub_grade")
).dropDuplicates(["sub_grade"]) \
 .withColumn("credit_grade_sk", monotonically_increasing_id() + 1)

dim_credit_grade = dim_credit_grade.select(
    "credit_grade_sk",
    "grade",
    "sub_grade"
)

## Loan Terms Dimension Mapping FKs and BKs, Generating Surrogate Keys
dim_loan_term = financial_df.select(
    col("term").alias("period")
).dropDuplicates(["period"]) \
 .withColumn("loan_term_sk", monotonically_increasing_id() + 1)

# Add Loan Term Description Column
dim_loan_term = dim_loan_term.select(
    "loan_term_sk",
    "period"
).withColumn(
    "term_description",
    concat_ws(" ", col("period"), lit("months"))
)

# Generate all dates for 2021
start_date = datetime(2021, 1, 1)
end_date = datetime(2025, 12, 31)

date_list = [(start_date + timedelta(days=x),) for x in range((end_date - start_date).days + 1)]

df_dates = spark.createDataFrame(date_list, ["Date"])

# Add Date_key in YYYYMMDD format
df_dates = df_dates.withColumn("Date_key", F.date_format(col("Date"), "yyyyMMdd").cast(IntegerType()))

# Extract Year, Month, Month_name, Quarter
df_dates = df_dates.withColumn("Year", year(col("Date"))) \
                   .withColumn("Month", month(col("Date"))) \
                   .withColumn("Month_name", date_format(col("Date"), "MMMM")) \
                   .withColumn("Quarter", F.quarter(col("Date")))

# Reorder columns
df_dates = df_dates.select("Date_key", "Date", "Year", "Month", "Month_name", "Quarter")


fact_loan_wip = financial_df \
    .join(df_dates.alias("d_issue"), col("issue_date_dt") == col("d_issue.Date"), "left") \
    .join(df_dates.alias("d_last_pay"), col("last_payment_date_dt") == col("d_last_pay.Date"), "left") \
    .join(df_dates.alias("d_next_pay"), col("next_payment_date_dt") == col("d_next_pay.Date"), "left") \
    .join(df_dates.alias("d_credit_pull"), col("last_credit_pull_date_dt") == col("d_credit_pull.Date"), "left")

fact_loan_wip = fact_loan_wip \
    .join(dim_borrowers, fact_loan_wip.member_id == dim_borrowers.borrowers_id_bk, "left") \
    .join(dim_status, fact_loan_wip.loan_status == dim_status.status_id, "left") \
    .join(dim_credit_grade, fact_loan_wip.sub_grade == dim_credit_grade.sub_grade, "left") \
    .join(dim_loan_term, fact_loan_wip.term == dim_loan_term.period, "left")


fact_loan = fact_loan_wip.select(
    # Business Key
    col("id").alias("loan_id_bk"),
    
    # Foreign Keys from Dimension Tables
    col("borrowers_id_sk").alias("borrowers_id_fk"),
    col("status_id_sk").alias("status_id_fk"),
    col("credit_grade_sk").alias("credit_grade_fk"),
    col("loan_term_sk").alias("loan_term_fk"),
    col("d_issue.Date_key").alias("date_key_issue"),
    col("d_last_pay.Date_key").alias("date_key_last_payment"),
    col("d_next_pay.Date_key").alias("date_key_next_payment"),
    col("d_credit_pull.Date_key").alias("last_credit_pull_date"),
    
    # Measures
    col("loan_amount"),
    col("dti").alias("DTI"),
    col("installment"),
    col("int_rate").alias("interest_rate"),
    col("total_payment"),
    col("purpose").alias("loan_purpose")
).withColumn("loan_id_pk_sk", monotonically_increasing_id() + 1)


fact_loan = fact_loan.select(
    "loan_id_pk_sk",
    "loan_id_bk",
    "borrowers_id_fk",
    "status_id_fk",
    "credit_grade_fk",
    "loan_term_fk",
    "date_key_issue",
    "date_key_last_payment",
    "date_key_next_payment",
    "last_credit_pull_date",
    "loan_amount",
    "DTI",
    "installment",
    "interest_rate",
    "total_payment",
    "loan_purpose"
)

spark.sql("USE default")

# Overwrite Hive tables Data to its Location automatically
dim_borrowers.write.mode("overwrite").insertInto("Dim_Borrowers", overwrite=True)
dim_credit_grade.write.mode("overwrite").insertInto("Dim_Credit_Grade", overwrite=True)
dim_status.write.mode("overwrite").insertInto("Dim_Status", overwrite=True)
dim_loan_term.write.mode("overwrite").insertInto("Dim_Loan_Term", overwrite=True)
df_dates.write.mode("overwrite").insertInto("Dim_Date", overwrite=True)
fact_loan.write.mode("overwrite").insertInto("Fact_Loan", overwrite=True)
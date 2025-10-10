from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
# from tasks.jobs import *
import logging

default_args = {
    "owner": "Mohamed Nasr",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="Loan_Analysis_ELT_Pipeline",
    start_date=datetime(2025, 10, 8),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=["stock_market"]
)
def Loans_ELT_Pipeline():    

    fetch_task = PythonOperator(
        task_id='get_stock_prices',
        python_callable = fetch_stock_data,
        provide_context=True        
    )
    
    # Task 4: Create Hive table
    create_hive_tables = BashOperator(
        task_id='Create_Hive_Fact_Dimensions',
        bash_command="""
        cat > /tmp/create_table.sql << 'SQL'
CREATE TABLE IF NOT EXISTS real_estate_sales (
    SaleID INT,
    List_Year INT,
    Town STRING,
    Address STRING,
    Assessed_Value DOUBLE,
    Sale_Amount DOUBLE,
    Sales_Ratio DOUBLE,
    Property_Type STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/real_estate/'
TBLPROPERTIES ('skip.header.line.count'='1');
SQL

        docker cp /tmp/create_table.sql hiveserver2:/tmp/create_table.sql
        docker exec hiveserver2 /opt/hive/bin/beeline \
            -u jdbc:hive2://localhost:10000 \
            -f /tmp/create_table.sql --silent=true
        echo "Hive table created successfully"
        """
    )
    
    # Task 5: Load data into Hive
    spark_transform_data = BashOperator(
        task_id='Load_Hive_Table',
        bash_command="""
        cat > /tmp/load_data.sql << 'SQL'
    LOAD DATA INPATH '/user/hive/warehouse/real_estate/real_estate_sales.csv' 
    INTO TABLE real_estate_sales;

    SELECT COUNT(*) as total_rows FROM real_estate_sales;
    SQL

            docker cp /tmp/load_data.sql hiveserver2:/tmp/load_data.sql
            docker exec hiveserver2 /opt/hive/bin/beeline \
                -u jdbc:hive2://localhost:10000 \
                -f /tmp/load_data.sql
            echo "Data loaded into Hive table successfully"
            """
        )
    
    
    
    fetch_task >> create_hive_tables >> spark_transform_data


Loans_ELT_Pipeline()

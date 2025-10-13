from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

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
    tags=["ELT", "Finance"]
)
def Loans_ELT_Pipeline():    
    
    # Task 4: Create Hive table
    create_hive_tables = BashOperator(
        task_id='Create_Hive_Fact_Dimensions',
        bash_command="""            
        docker exec -i ELT_Loan_hive-server bash -c 'hive -f /etc/Hive-DWH-DDL.sql'
        echo "Hive table created successfully"
        """
    )
    
    # Task 5: Load data into Hive
    spark_transform_data = BashOperator(
        task_id='transform_data',
        bash_command="""        
            docker exec -i ELT_Loan_namenode bash -c 'spark-submit --master spark://namenode:7077 /root/airflow/dags/spark.py' 
            echo "Data has been transformed and loaded into Data warehouse successfully!"
            """
        )
        
    create_hive_tables >> spark_transform_data

Loans_ELT_Pipeline()

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

with DAG(
    'test_bq_terraform_dev_connection',
    default_args=default_args,
    description='Tests the mounted JSON key connection to the Terraform dev dataset',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['terraform', 'dev', 'test'],
) as dag:

    # A simple query to test "Editor" permissions by creating a temporary table
    test_query = """
    CREATE TABLE IF NOT EXISTS `gcp-terraform-tmp.dev_raw_gci_marketing.airflow_hello_world` (
        test_id INT64,
        test_message STRING,
        created_at TIMESTAMP
    );
    INSERT INTO `gcp-terraform-tmp.dev_raw_gci_marketing.airflow_hello_world` (test_id, test_message, created_at)
    VALUES (1, 'Hello from Local Airflow Dev!', CURRENT_TIMESTAMP());
    """

    execute_bq_query = BigQueryInsertJobOperator(
        task_id='create_and_insert_test_table',
        gcp_conn_id='gcp_terraform_dev',
        configuration={
            "query": {
                "query": test_query,
                "useLegacySql": False,
            }
        }
    )

    execute_bq_query
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession, functions

def processo_etl_spark():
    spark = SparkSession \
        .builder \
        .appName("Extração Documentos a Pagar") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.uri", "mongodb://mongo1:27017/mydb.user?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://mongo1:27017/mydb.user?readPreference=primaryPreferred") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.network.timeout", 10000000) \
        .config("spark.executor.heartbeatInterval", 10000000) \
        .config("spark.storage.blockManagerSlaveTimeoutMs", 10000000) \
        .config("spark.executor.memory", "2g") \
        .master("spark://spark:7077") \
        .getOrCreate()

    data = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri","mongodb://mongo1:27017/mydb.step?authSource=admin").load()
    print(data.count())

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}
with airflow.DAG('dag_teste_spark_documento_vencido_v01',
                  default_args=default_args,
                  schedule_interval=timedelta(1)) as dag:
    task_elt_documento_pagar = PythonOperator(
        task_id='elt_documento_pagar_spark',
        python_callable=processo_etl_spark
    )

import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pyspark
from pyspark.sql import SparkSession
from airflow.models import DAG
from datetime import datetime, timedelta
now = datetime.now()


args = {
    'owner': 'Hassan',
    "start_date": datetime(now.year, now.month, now.day),
    'provide_context': True,
    'depends_on_past': True,
    'wait_for_downstream': True,
}

dag = DAG(
    dag_id='EL_MongoToRaw',
    default_args=args,
    schedule_interval=timedelta(1),
    max_active_runs=1,
    concurrency=1
)


# -----------------------------------------------------------------------------------------
# inner functions
def create_spark_session(spark_host):
    app = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.mongodb.input.uri", "mongodb://mongo1:27017/mydb.user?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://mongo1:27017/mydb.user?readPreference=primaryPreferred") \
        .master("spark://spark:7077") \
        .getOrCreate()
    return app

def read_db(url, spark_app):
    read_connection = spark_app.read.format("com.mongodb.spark.sql.DefaultSource")
    add_url = read_connection.option("spark.mongodb.input.uri", url)
    read_data = add_url.load()
    return read_data


# -----------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------

# task functions

# -----------------------------------------------------------------------------------------
def compress_to_raw(**kwargs):
    url = "mongodb://mongo1:27017/mydb.step?authSource=admin"
    # read data from MongoDB
    spark = create_spark_session("spark.mongodb.input.uri")
    data = read_db(url, spark)
    if (data != None):
        print("loaded data from MongoDB successfully.")
    else:
        print("check the data in MongoDB. It does'nt exist")
    # compress and partition data by partition_col value
    data.write.format("parquet").save("hdfs://namenode:9000//EDL_Data/Raw_Data_Zone/steps.parquet")

    # data\
    #     .withColumn("year", f.year(from_unixtime(f.col(partition_col)/1000).cast("timestamp")))\
    #     .withColumn("month", f.month(from_unixtime(f.col(partition_col)/1000).cast("timestamp")))\
    #     .withColumn("day", f.dayofmonth(from_unixtime(f.col(partition_col)/1000).cast("timestamp")))\
    #     .write\
    #     .partitionBy("year", "month", "day")\
    #     .mode("overwrite")\
    #     .option("compression", "snappy")\
    #     .parquet(raw_path)


# -----------------------------------------------------------------------------------------
# operators
t_compress_to_raw = PythonOperator(
    task_id='compress_to_raw',
    python_callable=compress_to_raw,
    dag=dag, )

t_compress_to_raw

import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import pyspark
from pyspark.sql import SparkSession
from airflow.models import DAG
import pyspark.sql.functions as f
from functools import reduce
from pyspark.sql.functions import from_unixtime, col
from datetime import datetime, timedelta
from pyspark.sql import functions as F
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
        .config("spark.mongodb.input.uri", "mongodb://mongo1:27017/mydb?readPreference=primaryPreferred") \
        .config("spark.mongodb.output.uri", "mongodb://mongo1:27017/mydb?readPreference=primaryPreferred") \
        .master(spark_host) \
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
def compress_to_raw_steps(**kwargs):
    url = "mongodb://mongo1:27017/mydb.step?authSource=admin"
    # read data from MongoDB
    spark = create_spark_session("spark://spark:7077")
    data = read_db(url, spark)
    if (data != None):
        print("loaded data from MongoDB successfully.")
    else:
        print("check the data in MongoDB. It does'nt exist")
    # compress and partition data by createdate value
    data.withColumn("Curr_date", F.lit(datetime.now().strftime("%Y-%m-%d")))\
        .write\
        .partitionBy("Curr_date")\
        .mode("overwrite")\
        .option("compression", "snappy")\
        .parquet("hdfs://namenode:9000//EDL_Data/Raw_Data_Zone/steps")
# -----------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------
def compress_to_raw_bloodpressure(**kwargs):
    url = "mongodb://mongo1:27017/mydb.bloodpressure?authSource=admin"
    # read data from MongoDB
    spark = create_spark_session("spark://spark:7077")
    data = read_db(url, spark)
    if (data != None):
        print("loaded data from MongoDB successfully.")
    else:
        print("check the data in MongoDB. It does'nt exist")
    # compress and partition data by createdate value
    data.withColumn("Curr_date", F.lit(datetime.now().strftime("%Y-%m-%d")))\
        .write\
        .partitionBy("Curr_date")\
        .mode("overwrite")\
        .option("compression", "snappy")\
        .parquet("hdfs://namenode:9000//EDL_Data/Raw_Data_Zone/bloodpressure")
# ---------------------------------------------------------------------------------------
# -----------------------------------------------------------------------------------------
def compress_to_raw_user(**kwargs):
    url = "mongodb://mongo1:27017/mydb.user?authSource=admin"
    # read data from MongoDB
    spark = create_spark_session("spark://spark:7077")
    data = read_db(url, spark)
    if (data != None):
        print("loaded data from MongoDB successfully.")
    else:
        print("check the data in MongoDB. It does'nt exist")
    # compress and partition data by createdate value
    data.createOrReplaceTempView ( "user" )
    datasql=spark.sql("select _id,givenName,familyName,email from user")
    datasql.withColumn("Curr_date", F.lit(datetime.now().strftime("%Y-%m-%d")))\
        .write\
        .partitionBy("Curr_date")\
        .mode("overwrite")\
        .option("compression", "snappy")\
        .parquet("hdfs://namenode:9000//EDL_Data/Raw_Data_Zone/user")
# ---------------------------------------------------------------------------------------
# operators
t_compress_to_raw_steps = PythonOperator(
    task_id='compress_to_raw_steps',
    python_callable=compress_to_raw_steps,
    dag=dag, )

t_compress_to_raw_bloodpressure = PythonOperator(
    task_id='compress_to_raw_bloodpressure',
    python_callable=compress_to_raw_bloodpressure,
    dag=dag, )
t_compress_to_raw_user = PythonOperator(
    task_id='compress_to_raw_user',
    python_callable=compress_to_raw_user,
    dag=dag, )
t_compress_to_raw_user >> t_compress_to_raw_bloodpressure >>t_compress_to_raw_steps

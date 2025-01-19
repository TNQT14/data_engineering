import logging
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import os
import sys

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    DROP TABLE IF EXISTS spark_streams.created_users;
    """)
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
          id TEXT PRIMARY KEY,
          first_name TEXT,
          last_name TEXT,
          gender TEXT,
          address TEXT,
          post_code TEXT,
          email TEXT,
          username TEXT,
          dob TEXT,
          registered_date TEXT,
          phone TEXT,
          picture TEXT);
      """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        print(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        print(f"kafka dataframe created successfully {spark_df}")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df



def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    # Đọc dữ liệu từ Kafka
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')) \
        .select("data.*")

    # Ép kiểu dữ liệu trước khi ghi vào Cassandra
    sel_casted = sel.withColumn("id", col("id").cast("string")) \
        .withColumn("first_name", col("first_name").cast("string")) \
        .withColumn("last_name", col("last_name").cast("string")) \
        .withColumn("gender", col("gender").cast("string")) \
        .withColumn("address", col("address").cast("string")) \
        .withColumn("post_code", col("post_code").cast("string")) \
        .withColumn("email", col("email").cast("string")) \
        .withColumn("dob", col("dob").cast("string")) \
        .withColumn("username", col("username").cast("string")) \
        .withColumn("registered_date", col("registered_date").cast("string")) \
        .withColumn("phone", col("phone").cast("string")) \
        .withColumn("picture", col("picture").cast("string"))

    print("DataFrame after casting:")
    sel_casted.printSchema()
    #     # print(sel_casted)
    if sel_casted.isStreaming:
        print("Spark DataFrame is streaming.")
    else:
        print("Spark DataFrame is not streaming or is empty.")

    return sel_casted


if __name__ == "__main__":
    try:
        spark_conn = create_spark_connection();
        print("Spark connection created successfully!")
    except Exception as e:
        print(f"Couldn't create the spark session due to exception {e}")

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)

            print("Streaming is being started...")

            streaming_query = selection_df.writeStream \
                            .format("org.apache.spark.sql.cassandra") \
                            .option("checkpointLocation", "/tmp/checkpoint") \
                            .option("keyspace", "spark_streams") \
                            .option("table", "created_users") \
                            .start()

            print(f"Streaming is being started 2...")

            streaming_query.awaitTermination()
            print("Streaming is being started 3...")

# docker-compose up -d
# $env:PYSPARK_PYTHON="D:/data_engineering/venv/Scripts/python.exe"
# spark-submit --master spark://localhost:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.kafka:kafka-clients:3.5.1 spark_stream.py

# docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
#SELECT * from spark_streams.created_users;
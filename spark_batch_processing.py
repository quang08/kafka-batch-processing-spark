from posixpath import split
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, window, size
from pyspark.sql.streaming import Trigger 

snowflake_options = {
    "sfURL": "https://<account>.snowflakecomputing.com",
    "sfAccount": "<account>",
    "sfWarehouse": "<warehouse>",
    "sfDatabase": "<database>",
    "sfSchema": "<schema>",
    "sfRole": "<role>",
    "sfUser": "<user>",
    "sfPassword": "<password>",
}

kafka_bootstrap_servers = "<kafka-bootstrap-servers>"
kafka_topic = "<kafka-topic>"

# Create a Spark session: specify the necessary packages (libraries) that enable Spark to communicate with these systems. 
spark = SparkSession \
    .builder \
    .appName("spark.jars.package",
             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,net.snowflake:snowflake-jdbc:3.13.4,net.snowflake:spark-snowflake_2.12:2.9.1") \
    .getOrCreate()

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
    .option('subscribe', kafka_topic) \
    .option('startingOffsets', 'earliest') \
    .load() \
    .selectExpr("CAST(value AS STRING)") # Cast the value column to a string: Kafka stores data in binary format

# Write to snowflake
def safe_write_to_snowflake(batch_df, batch_id):
    try:
        batch_df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", "target_table_name") \
            .mode("append") \
            .save()
    except Exception as e:
        print(f'Error writing batch {batch_id} to Snowflake: {e}')

def validate_data_quality(batch_df: DataFrame) -> DataFrame:
    # check for non null, non emoty, and correctly formatted names( assumes names contain exactly one space)
    validate_df = batch_df.filter(
        col('full_name').isNotNull() &
        col('full_name') != '' &
        size(split(col('full_name'), ' ')) == 2
    )

    return validate_df

# Batch processing
def process_micro_batch(batch_df, batch_id):
    validated_df = validate_data_quality(batch_df)

    # split the full name into first_name and last_name:
    processed_df = validated_df.withColumn('first_name', split(col('full_name'), " ").getItem(0)) \
                           .withColumn('last_name', split(col('full_name'), " ").getItem(1)) \
                           .drop('full_name') # drop the full_name column after splitting it into first_name and last_name
    
    # Attempt to write the processed data to Snowflake
    try: 
        processed_df.write \
                .format('snowflake') \
                .options(**snowflake_options) \
                .option("dbtable", "target_table_name") \
                .mode("append") \
                .save()
    except Exception as e:
        print(f'Error writing batch {batch_id} to Snowflake: {e}')

# Start the query that writes to Snowflake
query = kafka_df.writeStream \
        .foreachBatch(process_micro_batch) \
        .trigger(processingTime='1 minute') \
        .option('checkpointLocation', 'path/to/checkpoint/dir') \
        .start()
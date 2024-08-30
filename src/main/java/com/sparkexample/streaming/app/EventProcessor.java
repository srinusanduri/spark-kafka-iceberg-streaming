package com.sparkexample.streaming.app;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_timestamp;

public class EventProcessor {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        // Define base path for S3
        String basePath = "s3a://warehouse/";

        String awsAccessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
        String awsSecretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");

        // Create Spark session with Iceberg configurations
        SparkSession spark = SparkSession.builder()
                .appName("KafkaSparkIcebergStreaming")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "rest")
                .config("spark.sql.catalog.spark_catalog.uri", "http://rest:8181")
                .config("spark.sql.catalog.spark_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.spark_catalog.warehouse", basePath)
                .config("spark.sql.catalog.spark_catalog.s3.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
                .config("spark.hadoop.fs.s3a.access.key", awsAccessKeyId)
                .config("spark.hadoop.fs.s3a.secret.key", awsSecretAccessKey)
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        // Define schema for the incoming Kafka messages
        StructType schema = new StructType()
                .add("id", DataTypes.StringType)
                .add("timestamp", DataTypes.TimestampType)
                .add("value", DataTypes.DoubleType);

        // Read from Kafka
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "sample-stream-topic") // Kafka topic to subscribe to
                .option("startingOffsets", "earliest") // Start reading from the beginning of the topic
                .load();

        // Parse JSON data from Kafka
        Dataset<Row> parsedDF = df.select(
                        from_json(col("value").cast("string"), schema).alias("data"))
                .select("data.*");

        // Perform transformation: add a new column with doubled value
        Dataset<Row> transformedDF = parsedDF
                .withColumn("doubled_value", col("value").multiply(2))
                .withColumn("processing_time", current_timestamp());

        // Write to Iceberg table
        String tableName = "spark_catalog.default.streaming_data";

        // Create the table if it doesn't exist
        spark.sql("CREATE TABLE IF NOT EXISTS " + tableName + " " +
                "(id STRING, timestamp TIMESTAMP, value DOUBLE, " +
                "doubled_value DOUBLE, processing_time TIMESTAMP) " +
                "USING iceberg");

        // Write to Iceberg table using foreachBatch
        StreamingQuery query = transformedDF
                .writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    // Write the batch DataFrame to the Iceberg table
                    batchDF.writeTo(tableName).append();

                    // Optionally, log batch information
                    System.out.println("Processing batch: " + batchId + ", number of records: " + batchDF.count());
                })
                .option("checkpointLocation", basePath + "checkpoints/streaming_data/")
                .trigger(Trigger.ProcessingTime("30 seconds"))
                .start();

        query.awaitTermination();
    }

}
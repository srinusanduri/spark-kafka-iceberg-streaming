package com.sparkexample.streaming.app;

import ai.autonomic.feed.FeedMetricExporterOutputMessage;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.protobuf.functions.*;

public class TelemetryEventProcessor {

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

        // Read from Kafka
        Dataset<Row> df = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "feed-exported-metrics") // Kafka topic to subscribe to
                .option("startingOffsets", "earliest") // Start reading from the beginning of the topic
                .load();

        Map<String, String> options = new HashMap<>();
        options.put("recursive.fields.max.depth", "10");

        Dataset<Row> events = df.select(from_protobuf(col("value"),
                                "ai.autonomic.feed.FeedMetricExporterOutputMessage", options).as("event"))
                .select(col("event.*"))
                .select(
                        col("tmc_arrival_timestamp").cast("timestamp").as("tmc_arrival_timestamp"),
                        col("vin").as("vin_hash"),
                        col("data_uris").as("data_uris"),
                        col("device_type"),
                        col("region"),
                        col("protocol_version"),
                        col("flow_metric.flow_name").as("flow_name"),
                        col("flow_metric.flow_timestamp").cast("timestamp").as("flow_timestamp"),
                        col("correlation_ids"),
                        col("asset_id"),
                        col("expiration_length_types"),
                        year(current_timestamp()).as("year"),
                        month(current_timestamp()).as("month"),
                        day(current_timestamp()).as("day"),
                        hour(current_timestamp()).as("hour"),
                        col("flow_metric.flow_aui").as("flow_aui")
                );


//        Dataset<Row> events = df.select(df.col("value").as("bytes"))
//                .map(new MapFunction<Row, FeedMetricExporterOutputMessage>() {
//                    @Override
//                    public FeedMetricExporterOutputMessage call(Row row) throws Exception {
//                        byte[] bytes = row.getAs("bytes");
//                        return FeedMetricExporterOutputMessage.parseFrom(bytes);
//                    }
//                }, Encoders.bean(FeedMetricExporterOutputMessage.class))
//                .filter((FilterFunction<FeedMetricExporterOutputMessage>) row -> row != null)
//                .select(
//                        col("tmc_arrival_timestamp").cast("timestamp").as("tmc_arrival_timestamp"),
//                        col("vin").as("vin_hash"),
//                        col("data_uris").as("data_uris"),
//                        col("device_type"),
//                        col("region"),
//                        col("protocol_version"),
//                        col("flow_metric.flow_name").as("flow_name"),
//                        col("flow_metric.flow_timestamp").cast("timestamp").as("flow_timestamp"),
//                        col("correlation_ids"),
//                        col("asset_id"),
//                        col("expiration_length_types"),
//                        year(current_timestamp()).as("year"),
//                        month(current_timestamp()).as("month"),
//                        day(current_timestamp()).as("day"),
//                        hour(current_timestamp()).as("hour"),
//                        col("flow_metric.flow_aui").as("flow_aui")
//                );




        // Write to Iceberg table
        String tableName = "spark_catalog.default.flow_metrics_v3";

        // Create the table if it doesn't exist
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName + " (\n" +
                "  tmc_arrival_timestamp timestamp , \n" +
                "  vin_hash string , \n" +
                "  data_uris array<string> , \n" +
                "  device_type string , \n" +
                "  region string , \n" +
                "  protocol_version string , \n" +
                "  flow_name string , \n" +
                "  flow_timestamp timestamp , \n" +
                "  correlation_ids array<string> , \n" +
                "  asset_id string , \n" +
                "  expiration_length_types array<string>,\n" +
                "  year int , \n" +
                "  month int , \n" +
                "  day int , \n" +
                "  hour int,\n" +
                "  flow_aui string )\n" +
                "USING iceberg\n" +
                "PARTITIONED BY ( \n" +
                "  bucket(16,flow_aui),\n" +
                "  year, \n" +
                "  month, \n" +
                "  day, \n" +
                "  hour)";

        spark.sql(createTableDDL);

        // Write to Iceberg table
        events.writeStream()
                .outputMode("append")
                .foreachBatch((batchDF, batchId) -> {
                    // Write the batch DataFrame to the Iceberg table
                    batchDF.writeTo(tableName).append(); // Adjust the table name as needed
                })
                .option("path", basePath + "output/") // S3 output path
                .option("checkpointLocation", basePath + "checkpoints/flow_metrics_v3/")
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("1 minutes")) // Trigger every 1 minutes
                .start()
                .awaitTermination();
    }

}

package com.sparkexample.streaming.producer;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

public class WordCountStreamProcessor {

    public static void main(String[] args) throws InterruptedException, StreamingQueryException, TimeoutException {
        // Create a Spark session
        SparkConf conf = new SparkConf().setAppName("flow-metrics-stream-processor");
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Generate a stream of integers at a rate of 1 per second
        Dataset<Row> rateStream = spark.readStream()
                .format("rate")
                .option("rowsPerSecond", 1) // Adjust the rate as necessary
                .load();

        // Generate random words based on the rate stream
        Dataset<Row> wordsStream = rateStream.select(
                concat(
                        lit("word_"),
                        functions.col("value").cast("string")
                ).as("word"),
                functions.current_timestamp().as("timestamp")
        );


        // Define the output path for S3 or local
        String basePath = "s3://aws-emr-studio-131919519674-us-west-2/";
        // Uncomment the following line to use local path instead of S3
        // basePath = "/Users/ukarnati/workspace/autonomic-ai/eventstore-streaming-pipeline/";

        // Write the counts to S3 every 5 minutes
        wordsStream.writeStream()
                .outputMode("append") // Use "append" mode
                .format("csv") // Output format
                .option("path", basePath + "/output/") // S3 output path
                .option("checkpointLocation", basePath + "/checkpoint/") // Checkpoint location
                .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 minutes")) // Trigger every 5 minutes
                .start()
                .awaitTermination();
    }
}
package com.sparkexample.streaming.producer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class s3Copy {


    public static void main(String[] args)  {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Simple Spark App")
                .getOrCreate();


        // Path to the input CSV file in S3
        String inputPath = "s3://aws-emr-studio-131919519674-us-west-2/csv/people.csv";
        // Path to the output directory in S3
        String outputPath = "s3://aws-emr-studio-131919519674-us-west-2/csv/output/";

        // Read the CSV file
        Dataset<Row> df = spark.read()
                .option("header", "true") // Use the first line as header
                .csv(inputPath);

        // Show the DataFrame
        df.show();
        df.repartition(1);
        // Write the DataFrame to S3 in Parquet format
        df.write().mode("overwrite").parquet(outputPath);

        // Stop the Spark session
        spark.stop();
    }
}

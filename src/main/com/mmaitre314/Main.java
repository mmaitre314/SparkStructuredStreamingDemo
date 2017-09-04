package com.mmaitre314;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;

public class Main {

    public static void main(String[] args) throws Exception {

        System.out.printf("Current folder: %s\n", System.getProperty("user.dir"));

        SparkSession spark = SparkSession.builder()
            .appName("SparkStructuredStreamingDemo")
            .master("local[2]")
            .getOrCreate();

        Dataset<Row> stream = spark.readStream()
            .schema(new StructType()
                .add("Id", DataTypes.IntegerType)
                .add("Count", DataTypes.IntegerType))
            .csv("data\\input\\stream");

        Dataset<Row> reference = spark.read()
            .schema(new StructType()
                .add("Id", DataTypes.IntegerType)
                .add("Name", DataTypes.StringType))
            .csv("data\\input\\reference.csv");

        Dataset<Row> output = setupProcessing(spark, stream, reference);

        StreamingQuery query = output.writeStream()
                .format("csv")
                .outputMode(OutputMode.Append())
                .option("path", "data\\output")
                .option("checkpointLocation", "data\\checkpoint")
                .start();

        query.awaitTermination();
    }

    public static Dataset<Row> setupProcessing(SparkSession spark, Dataset<Row> stream, Dataset<Row> reference) {
        return stream.join(reference, "Id");
    }
}

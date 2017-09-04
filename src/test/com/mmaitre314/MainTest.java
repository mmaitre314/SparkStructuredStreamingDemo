package com.mmaitre314;

import scala.collection.JavaConversions;
import java.util.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.streaming.*;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class MainTest {

    private static SparkSession spark;

    @BeforeAll
    public static void setUpClass() throws Exception {
        spark = SparkSession.builder()
            .appName("SparkStructuredStreamingDemo")
            .master("local[2]")
            .getOrCreate();
    }

    @Test
    void testSetupProcessing() {
        Dataset<Row> stream = createStreamingDataFrame();
        Dataset<Row> reference = createStaticDataFrame();

        stream = Main.setupProcessing(spark, stream, reference);

        List<Row> result = processData(stream);

        assertEquals(3, result.size());
        assertEquals(RowFactory.create(2, 20, "Name2"), result.get(0));
    }

    private static List<Row> processData(Dataset<Row> stream) {
        stream.writeStream()
            .format("memory")
            .queryName("Output")
            .outputMode(OutputMode.Append())
            .start()
            .processAllAvailable();

        return spark.sql("select * from Output").collectAsList();
    }

    private static Dataset<Row> createStaticDataFrame() {
        return spark.createDataFrame(
                Arrays.asList(
                    RowFactory.create(1, "Name1"),
                    RowFactory.create(2, "Name2"),
                    RowFactory.create(3, "Name3"),
                    RowFactory.create(4, "Name4")
                ),
                new StructType()
                    .add("Id", DataTypes.IntegerType)
                    .add("Name", DataTypes.StringType));
    }

    private static Dataset<Row> createStreamingDataFrame() {
        MemoryStream<String> input = new MemoryStream<String>(42, spark.sqlContext(), Encoders.STRING());
        input.addData(JavaConversions.asScalaBuffer(Arrays.asList(
            "2,20",
            "3,30",
            "1,10")).toSeq());
        return input.toDF().selectExpr(
            "cast(split(value,'[,]')[0] as int) as Id",
            "cast(split(value,'[,]')[1] as int) as Count");
    }
}
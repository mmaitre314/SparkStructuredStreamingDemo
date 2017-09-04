# Unit Testing Spark Structured Streaming queries

This repo gives an example of unit test for [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) queries in Java. [IntelliJ](https://www.jetbrains.com/idea/) is used as IDE, [Maven](http://search.maven.org/) as package manager, and [JUnit](http://junit.org/) as test framework.

The query under test is a simple [stream enrichment](http://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-6/) where a stream of incoming events is joined with a reference dataset, here adding human-friendly names to event IDs.

```Java
public static Dataset<Row> setupProcessing(SparkSession spark, Dataset<Row> stream, Dataset<Row> reference) {
  return stream.join(reference, "Id");
}
```

## Unit Test

The unit test follows the Arrange/Act/Assert pattern.

```Java
@Test
void testSetupProcessing() {
    Dataset<Row> stream = createStreamingDataFrame();
    Dataset<Row> reference = createStaticDataFrame();

    stream = Main.setupProcessing(spark, stream, reference);
    List<Row> result = processData(stream);

    assertEquals(3, result.size());
    assertEquals(RowFactory.create(2, 20, "Name2"), result.get(0));
}
```

## Mocks

The main trick in this repo was to mock the inputs and outputs to isolate the query.

### Input stream

Two options work well:

- Using [MemoryStream](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/memory.scala) and defining the data in the code. Specifying a stream schema in `MemoryStream` did not look obvious to me, so I used rows of CSV strings that are parsed into typed columns using a SQL `SELECT` expression.

```Java
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
```

- Using a folder a CSV files

```Java
Dataset<Row> stream = spark.readStream()
    .schema(new StructType()
        .add("Id", DataTypes.IntegerType)
        .add("Count", DataTypes.IntegerType))
    .csv("data\\input\\stream");
```

### Input static dataset

Two options here too:

- Using an in-memory `List<Row>`

```Java
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
```

- Using a CSV file

```Java
Dataset<Row> reference = spark.read()
    .schema(new StructType()
        .add("Id", DataTypes.IntegerType)
        .add("Name", DataTypes.StringType))
    .csv("data\\input\\reference.csv");
```

### Output

There [MemorySink](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/memory.scala) is used to collect the output data into an `Output` table that is then queried to obtain a `List<Row>`.

```Java
private static List<Row> processData(Dataset<Row> stream) {
    stream.writeStream()
        .format("memory")
        .queryName("Output")
        .outputMode(OutputMode.Append())
        .start()
        .processAllAvailable();

    return spark.sql("select * from Output").collectAsList();
}
```

### Session

The last piece is the Spark session that hosts the data-processing pipeline locally.

```Java
@BeforeAll
public static void setUpClass() throws Exception {
    spark = SparkSession.builder()
        .appName("SparkStructuredStreamingDemo")
        .master("local[2]")
        .getOrCreate();
}
```

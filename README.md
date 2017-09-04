# Spark Structured Streaming Demo

This repo shows how to write unit tests for [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html) queries in Java. [IntelliJ](https://www.jetbrains.com/idea/) is used as IDE, [Maven](http://search.maven.org/) as package manager, and [JUnit](http://junit.org/) as test framework.

The function under test is a simple [stream enrichment](http://blog.madhukaraphatak.com/introduction-to-spark-structured-streaming-part-6/) where a stream of incoming events is joined with a reference data set (here adding human-friendly names to event IDs).

```Java
public static Dataset<Row> setupProcessing(SparkSession spark, Dataset<Row> stream, Dataset<Row> reference) {
  return stream.join(reference, "Id");
}
```




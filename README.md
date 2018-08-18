# spark-kafka-0-8-sql

Spark Structured Streaming Kafka Source for Kafka 0.8.

This library is design for Spark Structured Streaming Kafka source, its aim is to provide equal functionalities for users who still want to use Kafka 0.8/0.9.

### How to use

```scala

    import spark.implicits

    val reader = spark
      .readStream
      .format("kafka08")
      .option("kafka.bootstrap.servers", testUtils.brokerAddress)
      .option("startingoffset", "smallest")
      .option("topics", topic)

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("console")
      .trigger(ProcessingTime(2000L))
      .start()

    kafka.awaitTermination()

```

### To compile

This Structured Streaming Kafka 0.8 source is built with Maven, you could build with:

```
mvn clean package
```

### Compactible Spark Version

Due to the rigid changes of Structured Streaming component, This Kafka 0.8 Source can only worked with Spark after 2.0.2 and 2.2.0 and master branch.

### Important notes:

1. The schema of Kafka 0.8 source is fixed, you cannot change the schema of Kafka 0.8 source, this is different from most of other Sources in Spark.

    ```scala

        StructType(Seq(
        StructField("key", BinaryType),
        StructField("value", BinaryType),
        StructField("topic", StringType),
        StructField("partition", IntegerType),
        StructField("offset", LongType)))

    ```
2. You have to set `kafka.bootstrap.servers` or `kafka.metadata.broker` in Source creation.
3. You have to specify "topics" in Kafka 0.8 Source options, multiple topics are separated by ",".
4. All the Kafka related configurations set through Kafka 0.8 Source should be start with "kafka." prefix.
5. Option "startingoffset" can only be "smallest" or "largest".
6. Limit the number of messages by using spark.streaming.kafka.maxRatePerPartition 
7. Also supports batch mode, spark.read.format("kaka08")

# License

Apache License, Version 2.0 [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

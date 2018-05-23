package org.apache.spark.sql.kafka08.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object KafkaStreaming extends App {

  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("KafkaStreamingExample")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation" , "/tmp/uatest")
    .config("spark.streaming.kafka.maxRatePerPartition",5)
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val reader = sparkSession
    .readStream
    .format("kafka08")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingoffset", "smallest")
    .option("topics", "test1").load()

  val kafka = reader
    .selectExpr("CAST(value AS STRING)")
    .writeStream
    .format("console")
    .trigger(ProcessingTime(10000L))
    .start()

  kafka.awaitTermination()

}
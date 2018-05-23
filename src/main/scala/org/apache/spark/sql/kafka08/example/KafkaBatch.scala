package org.apache.spark.sql.kafka08.example

import org.apache.spark.sql.SparkSession

object KafkaBatch extends App{

  val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("kakaBatchExample")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation" , "/tmp/checkpoint")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  val dataFrame = sparkSession
    .read
    .format("kafka08")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("startingoffset", "smallest")
    .option("topics", "test1").load()

  dataFrame.selectExpr("CAST(value AS STRING)").show(false)

}

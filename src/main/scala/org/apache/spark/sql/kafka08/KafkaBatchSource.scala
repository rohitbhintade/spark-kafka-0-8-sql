package org.apache.spark.sql.kafka08

import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.{Broker, KafkaCluster, KafkaUtils, OffsetRange}

private[kafka08] class KafkaBatchSource(parameters: Map[String, String], userSchema: StructType)
                      (@transient val sqlContext: SQLContext) extends BaseRelation with Serializable with TableScan {

  import KafkaBatchSource._
  private val sc = sqlContext.sparkContext

  validateOptions(parameters)
  val caseInsensitiveParams: Map[String, String] = parameters.map { case (k, v) => (k.toLowerCase, v) }

  val specifiedKafkaParams: Map[String, String] = parameters.keySet
    .filter(_.toLowerCase.startsWith("kafka."))
    .map { k => k.drop(6).toString -> parameters(k) }
    .toMap

  val topics: Set[String] = caseInsensitiveParams.get(TOPICS) match {
    case Some(s) => s.split(",").map(_.trim).filter(_.nonEmpty).toSet
    case None => throw new IllegalArgumentException(s"$TOPICS should be set.")
  }

  private val kc = new KafkaCluster(specifiedKafkaParams)
  private val topicPartitions = KafkaCluster.checkErrors(kc.getPartitions(topics))

  override def schema: StructType = {
    if (this.userSchema != null)
      this.userSchema
    else
      kafkaSchema
  }

  override def buildScan(): RDD[Row] = {

    val fromPartitionOffsets =
      KafkaSourceOffset(KafkaCluster.checkErrors(kc.getEarliestLeaderOffsets(topicPartitions))).partitionToOffsets
    val untilPartitionOffsets =
      KafkaSourceOffset(KafkaCluster.checkErrors(kc.getLatestLeaderOffsets(topicPartitions))).partitionToOffsets

    val offsetRanges = fromPartitionOffsets.map{ case (tp, fo) =>
      val uo = untilPartitionOffsets(tp)
      OffsetRange(tp.topic, tp.partition, fo.offset, uo.offset)
    }.toArray

    val leaders = untilPartitionOffsets.map { case (tp, lo) =>
      tp -> Broker(lo.host, lo.port)
    }

    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) => {
      Row(mmd.key(), mmd.message(), mmd.topic, mmd.partition, mmd.offset)
    }

    // Create a RDD that reads from Kafka and get the (key, value) pair as byte arrays.
    val rdd = KafkaUtils.createRDD[
      Array[Byte],
      Array[Byte],
      DefaultDecoder,
      DefaultDecoder,
      Row](sc, specifiedKafkaParams, offsetRanges, leaders, messageHandler)

    rdd
  }
}

private[kafka08] object KafkaBatchSource {

  val TOPICS : String = "topics"
  private val STARTING_OFFSET_OPTION_KEY = "startingoffset"
  private val STARTING_OFFSET_OPTION_VALUES = Set("largest", "smallest")

  def kafkaSchema: StructType = StructType(Seq(
    StructField("key", BinaryType),
    StructField("value", BinaryType),
    StructField("topic", StringType),
    StructField("partition", IntegerType),
    StructField("offset", LongType)
  ))

  private def validateOptions(parameters: Map[String, String]): Unit = {

    // Validate source options
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase, v) }

    caseInsensitiveParams.get(STARTING_OFFSET_OPTION_KEY) match {
      case Some(pos) if !STARTING_OFFSET_OPTION_VALUES.contains(pos.trim.toLowerCase) =>
        throw new IllegalArgumentException(
          s"Illegal value '$pos' for option '$STARTING_OFFSET_OPTION_KEY', " +
            s"acceptable values are: ${STARTING_OFFSET_OPTION_VALUES.mkString(", ")}")
      case _ =>
    }

    // Validate user-specified Kafka options
    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.GROUP_ID_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.GROUP_ID_CONFIG}' is not supported as " +
          s"user-specified consumer groups is not used to track offsets.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}")) {
      throw new IllegalArgumentException(
        s"""
           |Kafka option '${ConsumerConfig.AUTO_OFFSET_RESET_CONFIG}' is not supported.
           |Instead set the source option '$STARTING_OFFSET_OPTION_KEY' to 'largest' or
           |'smallest' to specify where to start. Structured Streaming manages which offsets are
           |consumed internally, rather than relying on the kafkaConsumer to do it. This will
           |ensure that no data is missed when when new topics/partitions are dynamically
           |subscribed. Note that '$STARTING_OFFSET_OPTION_KEY' only applies when a new Streaming
           |query is started, and that resuming will always pick up from where the query left
           |off. See the docs for more
           |details.
         """.stripMargin)
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}")) {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG}' is not supported as keys "
          + "are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame operations "
          + "to explicitly deserialize the keys.")
    }

    if (caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}"))
    {
      throw new IllegalArgumentException(
        s"Kafka option '${ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG}' is not supported as "
          + "value are deserialized as byte arrays with ByteArrayDeserializer. Use DataFrame "
          + "operations to explicitly deserialize the values.")
    }

    val otherUnsupportedConfigs = Seq(
      "auto.commit.enable", // committing correctly requires new APIs in Source
      "zookeeper.connect")

    otherUnsupportedConfigs.foreach { c =>
      if (caseInsensitiveParams.contains(s"kafka.$c")) {
        throw new IllegalArgumentException(s"Kafka option '$c' is not supported")
      }
    }

    if (!caseInsensitiveParams.contains(s"kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}") &&
      !caseInsensitiveParams.contains(s"kafka.metadata.brokers.list")) {
      throw new IllegalArgumentException(
        s"Option 'kafka.${ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}' or " +
          "'kafka.metadata.broker.list' must be specified for configuring Kafka consumer")
    }
  }

}
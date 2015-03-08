package org.pilar.noaa.server

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.Minutes

/**
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.FluxServer zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 */
object FluxServer {
  def main(args: Array[String]) {
    FluxLogger.setStreamingLogLevels()

    val zkQuorum = "zoo1,zoo2"
    val topics = "test-message,test-message-1"
    val group = "my-consumer-group"

    val sparkConf = new SparkConf().setAppName("FluxServer")
    val ssc = new StreamingContext(sparkConf, Seconds(210))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, 2)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

object FluxLogger extends Logging {
  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example. To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
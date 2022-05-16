package com.epam.hryshko

import org.apache.commons.codec.StringDecoder
import org.apache.spark.sql.SparkSession

import scala.io.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.catalyst.expressions.StringDecode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaDStream
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies;

object Consumer {
  def main(args: Array[String]) = {
    var arrHostPort: Array[String] = Array(Constants.HOST_PORT)

    val censored = Source.fromFile(Constants.CENSORED)
    var arrCensored: Array[String] = Array()
    censored.getLines().foreach{ word => arrCensored :+= word }
    censored.close()




    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local[*]")
      .getOrCreate()


//    val spark = SparkSession
//      .builder
//      .appName("Spark-Kafka-Integration")
//      .master("local")
//      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1")
//      .getOrCreate()

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", arrHostPort.mkString(", "))
      .option("subscribe", "json_topic")
      .option("startingOffsets", "earliest") // From starting
      .load()

    df.printSchema()




  }

}


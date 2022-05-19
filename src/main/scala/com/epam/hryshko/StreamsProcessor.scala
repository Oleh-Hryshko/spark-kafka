package com.epam.hryshko


import org.apache.hadoop.thirdparty.protobuf.Timestamp

import java.time.{LocalDate, Period}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.nio.file.Paths

object StreamsProcessor {
  def main(args: Array[String]): Unit = {
    new StreamsProcessor(Constants.HOST_PORT).process()
  }
}

class StreamsProcessor(brokers: String) {

  def process(): Unit = {

    System.setProperty("hadoop.home.dir", "c:\\winutil")
//    val OS: String = System.getProperty("os.name").toLowerCase
//
//    if (OS.contains("win")) System.setProperty("hadoop.home.dir", Paths.get("winutil").toAbsolutePath.toString)
//    else System.setProperty("hadoop.home.dir", "/")

    val spark = SparkSession.builder()
      .appName("kafka")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._


    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", Constants.TOPIC)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "true")
      //.option("value.deserializer", "StringDeserializer")
      .load()

    val wordJsonDf = inputDf.selectExpr("CAST(value AS STRING)")
      //.toDF("value")

    //val wordJsonDf = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val struct = new StructType()
      .add("speaker", DataTypes.StringType)
      .add("word", DataTypes.StringType)
      .add("time", DataTypes.TimestampType)

    val messageNestedDf = wordJsonDf.select(from_json($"value", struct).as("message"))

    val messageDf = messageNestedDf.selectExpr("message.speaker", "message.word")
    //val messageDf = messageNestedDf.selectExpr("message.*")

    messageNestedDf.printSchema()


    val consoleOutput = messageDf.writeStream
      .outputMode("append")
      .format("console")
      .start()


    //   val kafkaOutput = resDf.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", brokers)
//      .option("topic", Constants.TOPIC)
//      .option("checkpointLocation", "/tmp")
//      .start()

    spark.streams.awaitAnyTermination()
  }

}

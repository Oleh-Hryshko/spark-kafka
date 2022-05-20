package com.epam.hryshko


import org.apache.hadoop.thirdparty.protobuf.Timestamp

import java.time.{LocalDate, Period}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{window, _}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataTypes, StructType}

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt
import scala.io.Source

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

    val censored = Source.fromFile(Constants.CENSORED)
    var arrCensored: Array[String] = Array()
    censored.getLines().foreach{ word => arrCensored :+= word }
    censored.close()

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

    //val wordJsonDf = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    val wordJsonDf = inputDf.selectExpr("CAST(value AS STRING)")
      .toDF("value")

    val struct = new StructType()
      .add("speaker", DataTypes.StringType)
      .add("word", DataTypes.StringType)
      .add("time", DataTypes.TimestampType)

    val messageNestedDf = wordJsonDf.select(from_json($"value", struct).as("message"))


    //val messageDf = messageNestedDf.selectExpr("message.time", "message.speaker", "message.word")
    val messageDf = messageNestedDf.selectExpr("message.*")
      .groupBy("speaker")
      .agg(collect_list("word") as "words")
      .select("words", "speaker", "time")



    messageNestedDf.printSchema()



    val consoleOutput = messageDf.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start()




    //val kafkaOutput = resDf.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", brokers)
//      .option("topic", Constants.TOPIC)
//      .option("checkpointLocation", "/tmp")
//      .start()

    spark.streams.awaitAnyTermination()
  }

}

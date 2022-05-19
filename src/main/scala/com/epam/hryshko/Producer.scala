package com.epam.hryshko

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties
import scala.io.Source
import scala.util.Random
import scala.util.control.Breaks.break

object Producer {

  def main(args: Array[String]) = {
    var arrHostPort: Array[String] = Array(Constants.HOST_PORT)

    val speakers = Source.fromFile(Constants.SPEAKERS)
    var arrSpeakers: Array[String] = Array()
    speakers.getLines().foreach{ speaker => arrSpeakers :+= speaker }
    speakers.close()

    val dictionary = Source.fromFile(Constants.DICTIONARY)
    var arrWords: Array[String] = Array()
    dictionary.getLines().foreach{ word => arrWords :+= word }
    dictionary.close()

    sendMessages(arrHostPort, Constants.TOPIC, arrSpeakers, arrWords)

  }

  def sendMessages(arrHostPort: Array[String], topic: String, arrSpeakers: Array[String], arrWord: Array[String]) {

    var time = System.currentTimeMillis()
    val producer = createKafkaProducer(arrHostPort.mkString(", "))
    val rnd = new Random()
    val numberOfMessages = 10
    var i = 1

    while (true) {

      for (counter <- arrSpeakers.indices) {
        time += 1000
        val timestamp = new java.sql.Timestamp(time).toString
        val speaker = arrSpeakers(rnd.nextInt(arrSpeakers.length - 1))
        val word = arrWord(rnd.nextInt(arrWord.length - 1))

        val message = "{" +
          "\n\t \"speaker\": \"" + speaker + "\"," +
          "\n\t \"time\": \"" + timestamp + "\"," +
          "\n\t \"word\": \"" + word + "\"" +
          "\n} "

        //      val message = "{" +
        //        "\"speaker\": \"" + speaker + "\", " +
        //        "\"time\": \"" + timestamp + "\", " +
        //        "\"word\": \"" + word + "\"" +
        //        "}"

        //val record = new ProducerRecord[String, String](topic, message)
        val record = new ProducerRecord[String, String](topic, null, time, speaker, message, null)


        try {
          producer.send(record)
          println(message)
        } catch {
          case ex: Error => {
            println(ex + ":" + speaker + " " + timestamp + " " + topic + " " + word)
          }
        }
      }
      i += 1
      if (i >= numberOfMessages) {
        break
      }
    }
    producer.close()

  }

  def createKafkaProducer(hostsPorts: String): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", hostsPorts)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("timestamp.serializer", "org.apache.kafka.common.serialization.LongSerializer")

    new KafkaProducer[String, String](props)
  }

}

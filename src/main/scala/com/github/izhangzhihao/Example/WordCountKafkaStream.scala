package com.github.izhangzhihao.Example

import java.net.InetAddress
import java.util.Properties
import java.util.regex.Pattern

import com.lightbend.kafka.scala.streams.DefaultSerdes._
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.{KTableS, StreamsBuilderS}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object WordCountKafkaStream extends App {

  val inputTopic = "textLines"
  val outputTopic = "wordcount"

  val host = InetAddress.getLocalHost.getHostAddress

  val brokers = s"$host:9092"
  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount")
  props.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcountgroup")
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(StreamsConfig.STATE_DIR_CONFIG, "local_state_data")
  props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val builder = new StreamsBuilderS()

  val textStream = builder
    .stream[String, String](inputTopic)

  val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

  val wordCounts: KTableS[String, Long] = textStream
    .flatMapValues(v => {
      println(v)
      pattern.split(v.toLowerCase())
    })
    .groupBy((_, v) => v)
    .count()


  wordCounts
    .toStream
    .map((word, count) => {
      println(s"$word : $count")
      (word, count.toString)
    })
    .to(outputTopic)

  val streams = new KafkaStreams(builder.build(), props)

  streams.start()

  Thread.sleep(50000)

  streams.close()
  System.exit(0)
}

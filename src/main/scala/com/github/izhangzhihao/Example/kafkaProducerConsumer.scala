package com.github.izhangzhihao.example

import java.net.InetAddress
import java.util.Arrays.asList
import java.util.concurrent.Executors

import com.github.izhangzhihao.kafka._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.JavaConversions.asExecutionContext
import scala.concurrent.{ExecutionContextExecutorService, Future}

object kafkaProducerConsumer extends App {

  val host = InetAddress.getLocalHost.getHostAddress

  val producer = kafkaProducer(s"$host:9092")

  implicit val context: ExecutionContextExecutorService = asExecutionContext(Executors.newScheduledThreadPool(4))

  Future {
    for (value <- 1 to 32) {
      val producerRecord = new ProducerRecord[String, String]("topic1", s"${DateTime.now().toString()}", value.toString)
      println(producer.send(producerRecord).get().offset())
      //producer.send(producerRecord, (metadata: RecordMetadata, exception: Exception) => println(metadata.offset()))
      Thread.sleep(500)
    }
  }


  val consumer = kafkaConsumer(s"$host:9092")
  consumer.subscribe(asList("topic1"))

  consumer.subscription().asScala.foreach(println)


  Thread.sleep(10000)


  val values = consumer.poll(1000)

  val iterable: List[ConsumerRecord[String, String]] = values.records("topic1").asScala.toList

  for (it <- iterable) {
    println(it.key() + " : " + it.value())
  }
  consumer.close()

  //consumer.pause(consumer.assignment())
  //consumer.resume(consumer.assignment())

  println("Consumer finished task")

  Runtime.getRuntime.addShutdownHook {
    new Thread() {
      override def run(): Unit = {
        println("Starting exit...")
        consumer.wakeup()
      }
    }
  }

  System.exit(0)
}

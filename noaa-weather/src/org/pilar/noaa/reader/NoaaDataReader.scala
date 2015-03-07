package org.pilar.noaa.reader

import java.util.Properties
import org.pilar.noaa.FluxChannel
import kafka.producer._
import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
 * @author marcin
 */
object NoaaDataReader {
  def processUrl(urlCode: String, url: String) : ListBuffer[FluxChannel] = {
    var result:ListBuffer[FluxChannel] = ListBuffer[FluxChannel]()

    try {
      val html = Source.fromURL(url)

      //print(html.getLines().mkString("/n"))

      for (line <- html.getLines()) {
        if (!line.startsWith("#") && !line.startsWith(":")) {
          val items = line.replace("   ", " ").replace("  ", " ").split(" ")

          result += (new FluxChannel(
            urlCode,
            items(0).toInt,
            items(1).toInt,
            items(2).toInt,
            items(3).toInt,
            items(6).toDouble,
            items(7).toDouble,
            items(8).toDouble,
            items(9).toDouble,
            items(10).toDouble,
            items(11).toDouble,
            items(12).toDouble,
            items(13).toDouble,
            items(14).toDouble,
            items(15).toDouble))
        }
      }
    } catch {
      //print stacktrace and ignore
      case t: Throwable => t.printStackTrace()
    }

    println(result.size)

    return result
  }

  def main(args: Array[String]) {

    val props = new Properties()
    props.put("metadata.broker.list", "52.10.104.227:9092")
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("request.required.acks", "1")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    val messages = processUrl("1", "http://services.swpc.noaa.gov/text/goes-magnetospheric-particle-flux-ts1-primary.txt")
    messages :+ processUrl("2", "http://services.swpc.noaa.gov/text/goes-magnetospheric-particle-flux-ts2-primary.txt")
    messages :+ processUrl("3", "http://services.swpc.noaa.gov/text/goes-magnetospheric-particle-flux-ts3-primary.txt")

    messages.foreach( message => {
      producer.send(new KeyedMessage[String, String]("test-topic", message.toMessage()))
    })

    producer.close()

    println("Done")
  }
}
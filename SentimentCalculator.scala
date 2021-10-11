import twitter4j.Status
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import SentimentFinder.sentiment

import java.util.Properties

object SentimentCalculator {
  def main(args: Array[String]) {

    // Setting logging level
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {Logger.getRootLogger.setLevel(Level.WARN)}

    if (args.length <=1 ) {
      System.out.println("Input: SentimentCalculator topic keyword1 keyword2")
      return
    }

    val broker = "localhost:9092"
    val top= args(0).toString
    val keys = args.slice(1, args.length)
    //val sc = new SparkConf().setAppName("Tweets-Sentiment-Analysis")
    val sc = new SparkConf().setAppName("tade_streamer")

    // setting master URL to local if not set
    if (!sc.contains("spark.master")) {sc.setMaster("local[2]")}

    val myStream = new StreamingContext(sc, Seconds(6))

    val builder = new ConfigurationBuilder
    builder.setDebugEnabled(true).setOAuthConsumerKey("JIaJOF3Mv6fjanxqff8yZbcaG").setOAuthConsumerSecret("a7k468nFMSiM3fdhOnv2wPv3R3lN1niMl6yXKKso3au8KRQkzH")
      .setOAuthAccessToken("165085641-VtIBnej3vZE0I5AJ8nu99v4BuEcIG0").setOAuthAccessTokenSecret("myPJ11jGNi8gjRMXHcgC2lH3UgLo336w1JlE87iUMsxnI")

    val authentication = new OAuthAuthorization(builder.build)

    val data: DStream[Status] = TwitterUtils.createStream(myStream, Some(authentication), keys)

    val sentiments: DStream[Int] = data.filter(a => a.getLang == "en").map(_.getText).map(word => (sentiment(word)))

    val sentimentText = Array("Very Negative", "Negative", "Neutral", "Positive", "Very Positive")

    // sending  inputs to broker

    sentiments.foreachRDD( input => {

      input.foreachPartition( part => {

        val properties = new Properties()

        val bs = "localhost:9092"

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)

        properties.put("bootstrap.servers", bs)

        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val prod = new KafkaProducer[String, String](properties)

        part.foreach( rec => {

          val info = sentimentText(rec)

          val msg = new ProducerRecord[String, String](top, null, info)

          prod.send(msg)

        }
        )
        prod.close()

      }
      )

    }
    )
    myStream.start()

    myStream.awaitTermination()

  }
}


import com.datastax.spark.connector.cql.CassandraConnector
import kafka.serializer.StringDecoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import org.apache.spark.sql.SaveMode._
import org.json4s.jackson.Json

object StreamingConsumer {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("kafkaconsumer")

    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    val ssc = new StreamingContext(sparkContext, Seconds(2))
    val cassandraSettings = CassandraConnSettings(sparkContext)

    //config kafka

    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topic = Set("test")
    val directKafkaStreams = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic)

    directKafkaStreams.map(event => Row(event._2)).foreachRDD(rdd => {
      cassandraSettings.save(sqlContext, rdd)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

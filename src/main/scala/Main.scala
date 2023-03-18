
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._

import java.util.Properties

object Main {
  private val bootstrapServer = "localhost:29092"
  private val topicInput = "input"
  private val topicOutput = "predictition"
  val modelPath = "./src/main/resources/modelLast/"

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("HomeWork7")
      .setMaster("local[2]")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "file:///home/vadim/MyExp/spark-logs/event")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.sparkContext.setLogLevel("ERROR")

    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServer)


    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServer,
      ConsumerConfig.GROUP_ID_CONFIG -> "group1",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicInput), kafkaParams))

    val modelLoaded = DecisionTreeModel.load(ssc.sparkContext, modelPath)

    messages
      .foreachRDD(rdd =>
        rdd
          .filter(c => c.value.nonEmpty)
          .foreach(
            record => {
              val v =record.value.split(",")
              val prediction = modelLoaded.predict(Vectors.dense(v(0).toDouble, v(1).toDouble, v(2).toDouble, v(3).toDouble)) match {
                case 0.0 => "Iris-setosa"
                case 1.0 => "Iris-versicolor"
                case 2.0 => "Iris-virginica"
                case _ => "Unknown"
              }
              println(s"${v.mkString("Array(", ", ", ")")},$prediction")
              val producer = new KafkaProducer(props, new StringSerializer, new StringSerializer)
              producer.send(new ProducerRecord(topicOutput, "key", s"${v.mkString(",")},$prediction"))
              producer.close()
            }
          )
      )

    ssc.start()
    ssc.awaitTermination()

  }
}
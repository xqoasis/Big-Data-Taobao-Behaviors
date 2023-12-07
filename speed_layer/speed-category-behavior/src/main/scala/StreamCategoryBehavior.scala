import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

object StreamCategoryBehavior {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("latest_xqoasis_category_behavior_sum"))

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: StreamFlights <brokers>
                            |  <brokers> is a list of one or more Kafka brokers
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamBehavior")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("xqoasis_user_behavior")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[CategoryBehaviorReport]))

    // How to write to an HBase table
    val batchStats = reports.map(cbr => {
      val put = new Put(Bytes.toBytes(cbr.category_id))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("pv"), Bytes.toBytes(cbr.pv))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fav"), Bytes.toBytes(cbr.fav))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("cart"), Bytes.toBytes(cbr.cart))
      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("buy"), Bytes.toBytes(cbr.buy))
      table.put(put)
    })
    batchStats.print()

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
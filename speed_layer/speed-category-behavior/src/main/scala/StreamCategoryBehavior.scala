import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes

object StreamCategoryBehavior {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val CategoryMap = hbaseConnection.getTable(TableName.valueOf("xqoasis_category_id_desc"))
  val CategoryBehavior = hbaseConnection.getTable(TableName.valueOf("xqoasis_category_behavior_sum"))
  val UserBehavior = hbaseConnection.getTable(TableName.valueOf("xqoasis_user_behavior_count_score"))

  def incrementCategoryBehavior(ubr : UserBehaviorRecord) : String = {
    val maybeCategoryDesc = CategoryMap.get(new Get(Bytes.toBytes(ubr.category_id)))
    if(maybeCategoryDesc.isEmpty)
      return "No category for id" + ubr.category_id;
    val categoryDesc = maybeCategoryDesc.getValue(Bytes.toBytes("desc"), Bytes.toBytes("category_desc"))
    val inc = new Increment(categoryDesc)
    val ub_inc = new Increment(Bytes.toBytes(ubr.user_id))
    if(ubr.pv_val){
      inc.addColumn(Bytes.toBytes("behaviorSum"), Bytes.toBytes("pv"), 1)
      ub_inc.addColumn(Bytes.toBytes("info"), Bytes.toBytes("pv"), 1)
    }
    if(ubr.fav_val) {
      inc.addColumn(Bytes.toBytes("behaviorSum"), Bytes.toBytes("fav"), 1)
      ub_inc.addColumn(Bytes.toBytes("info"), Bytes.toBytes("fav"), 1)
    }
    if(ubr.cart_val) {
      inc.addColumn(Bytes.toBytes("behaviorSum"), Bytes.toBytes("cart"), 1)
      ub_inc.addColumn(Bytes.toBytes("info"), Bytes.toBytes("cart"), 1)
    }
    if (ubr.buy_val) {
      inc.addColumn(Bytes.toBytes("behaviorSum"), Bytes.toBytes("buy"), 1)
      ub_inc.addColumn(Bytes.toBytes("info"), Bytes.toBytes("buy"), 1)
    }
    CategoryBehavior.increment(inc)
    UserBehavior.increment(ub_inc)
    return "finished increment for" + ubr.category_id + " " + ubr.user_id;
  }

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
    val sparkConf = new SparkConf().setAppName("StreamCategoryBehavior")
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

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[UserBehaviorRecord]))

    // Update speed table
    val processedFlights = kfrs.map(incrementCategoryBehavior)

    // How to write to an HBase table
//    val batchStats = reports.map(cbr => {
//      val put = new Put(Bytes.toBytes(cbr.category_id))
//      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("pv"), Bytes.toBytes(cbr.pv))
//      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("fav"), Bytes.toBytes(cbr.fav))
//      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("cart"), Bytes.toBytes(cbr.cart))
//      put.addColumn(Bytes.toBytes("weather"), Bytes.toBytes("buy"), Bytes.toBytes(cbr.buy))
//      CategoryBehavior.put(put)
//    })
//    batchStats.print()
    processedFlights.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}

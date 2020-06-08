import java.util.{Properties, UUID}
import scala.util.parsing.json._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.json4s.{Formats,NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
object main_02 {
  /**
   * 输入的主题名称
   */
  val inputTopic = "mn_buy_ticket_1"
  /**
   * kafka地址
   */
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
    val events = inputKafkaStream.keyBy("")
    case class userLableLike(id:String,pos:Float,neg:Float,seg:Double)
    def userLable2Str(data:userLableLike): String ={
      // 需要添加隐式转换
      implicit val formats:AnyRef with Formats = Serialization.formats(NoTypeHints)
      // 由scala对象转换为Json字符串
      val dstr = write(data)
      dstr
    }
    inputKafkaStream.map {
       x=>
        val m = JSON.parseFull(x) match {
          case Some(map: Map[String, Any]) => map
        }
    }.keyBy(_._1).sum(1)

    env.execute()
  }

}

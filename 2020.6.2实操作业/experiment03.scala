import java.sql.{ResultSet, SQLException}

import java.util.Properties



import com.bingocloud.auth.BasicAWSCredentials

import com.bingocloud.services.s3.AmazonS3Client

import com.bingocloud.util.json.JSONException

import com.bingocloud.{ClientConfiguration, Protocol}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.json.*
import org.nlpcn.commons.lang.util.IOUtil



object Main {
  //kafka参数

  val topic = "chensi"

  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"



  def main(args: Array[String]): Unit = {

    val tableContent = dataFromMysql()

    produceToKafka(tableContent)

  }


   // 从Mysql获取表数据


  def dataFromMysql(): String = {

    import java.sql.DriverManager

    val url = "jdbc:mysql://bigdata28.depts.bingosoft.net:23307/user05_db"



    val properties = new Properties()

    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")

    properties.setProperty("user", "user05")

    properties.setProperty("password", "pass@bingo5")



    val connection = DriverManager.getConnection(url, properties)

    val statement = connection.createStatement

    val resultSet = statement.executeQuery("select * from m")

    try {



      val res = rsJson(resultSet)

      resultSet.close()

      res

    } catch {

      case e: Exception => e.printStackTrace()

        return "0"

    }

  }


    



  def produceToKafka(s3Content: String): Unit = {

    val props = new Properties

    props.put("bootstrap.servers", bootstrapServers)

    props.put("acks", "all")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val dataArr = s3Content.split("\n")

    for (s <- dataArr) {

      if (!s.trim.isEmpty) {

        val record = new ProducerRecord[String, String](topic, null, s)

        println("开始生产数据：" + s)

        producer.send(record)

      }

    }

    producer.flush()

    producer.close()

  }

}
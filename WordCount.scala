package net.bingosoft.spark.demo
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object WordCount {
  def main(args: Array[String]): Unit = {
    //spark的相关配置
    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-common-2.2.0-bin-master")
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //对结果增加过滤,支持词性过滤,和词语过滤
    val filter = new StopRecognition()
    filter.insertStopNatures("w")

    //从西游记小说中读取数据
    val rdd = sc.textFile("file:///D:/大数据实训/experiment02/西游记.txt")
      .map { x =>
        val str = if (x.length > 0)
        //开始分词，将结果使用逗号隔开
        ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(",")
        str.toString
      }.flatMap(x => x.split(","))
    val result = rdd.map((_, 1)).groupBy(_._1).mapValues(_.size).filter(x => StringUtils.isNotBlank(x._1))
    //result.foreach(println)
    //统计如来和孙悟空出现的次数并输出结果
    val result01= result.filter(x => x._1.equals("如来")).collect()
    val result02 = result.filter(x => x._1.equals("孙悟空")).collect()
    println(result01(0)._2+result02(0)._2)




    //val b= result02.values.collect()

    //println("如来+悟空"+)

    //val sorted = result.sortBy(_._2, false).filter(x=>x._1.length>=2)
    //输出前五个最多的词
    //sorted.collect().take(5).foreach(println)
    //输出所有结果到本文中
    //sorted.map(x=>{x._1+","+x._2}).saveAsTextFile("file:///D:/output"+System.nanoTime())

  }


}

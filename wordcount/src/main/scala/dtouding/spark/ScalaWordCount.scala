package dtouding.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    //创建Spark配置，设置应用名称
    //val conf = new SparkConf().setAppName("ScalaWordCount") //提交到集群
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("local[2]") //本地执行
    //创建Spark执行的入口
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词和一组合成元组
    val wordTuples: RDD[(String, Int)] = words.map((_, 1))
    //按key进行聚合
    val reducedWords: RDD[(String, Int)] = wordTuples.reduceByKey(_+_)
    //排序
    val sortedWords: RDD[(String, Int)] = reducedWords.sortBy(_._2, false)
    //将结果保存到HDFS中
    sortedWords.saveAsTextFile(args(1))
    //释放资源
    sc.stop()
  }
}

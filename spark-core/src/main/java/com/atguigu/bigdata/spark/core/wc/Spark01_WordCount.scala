package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //app 连接spark环境 将功能传递给它执行
    //TODO 建立和Spark框架得连接
    //JDBC:Connection
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf) //增加和spark的依赖关系 或者 提供jar包
    //TODO 执行业务操作

    //1.读取文件，获取一行一行的数据
    //hello world
    val lines: RDD[String] = sc.textFile("datas")

    //2.将一行数据进行拆分，形成一个一个的单词(分词) --- 扁平化操作
    //扁平化：将整体拆分成个体的操作
    //"hello world"... => hello,world,hello,world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //3.将数据根据单词进行分组，便于统计
    //(hello,hello,hello),(world,world)
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word) //Iterable可迭代的

    //    println(wordGroup)
    //4.对分组后的数据进行聚合
    //(hello,3),(world,2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5.将转换结果采集到控制台打印出来
    wordGroup.collect().foreach(println)
    println("**********************")
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()

  }
}

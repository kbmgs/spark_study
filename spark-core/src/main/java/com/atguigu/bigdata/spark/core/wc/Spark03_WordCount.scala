package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //app 连接spark环境 将功能传递给它执行
    //TODO 建立和Spark框架得连接
    //JDBC:Connection
    val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparConf) //增加和spark的依赖关系 或者 提供jar包
    //TODO 执行业务操作


    val lines: RDD[String] = sc.textFile("datas")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    //优化算法，增加预聚合 --- hello,hello,world => (hello,1)(hello,1)(world,1)
    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)   //.map(_,1)
    )
    //Spark框架提供了更多的功能。可以将分组和聚合使用一个方法实现。
    //reduceByKey：相同的key的数据，可以对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey((x, y) => x + y) //.reduceByKey(_+_)

    //5.将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop()

  }
}

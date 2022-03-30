package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
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
      word => (word, 1)
    )

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
      word => word._1
    )

    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
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

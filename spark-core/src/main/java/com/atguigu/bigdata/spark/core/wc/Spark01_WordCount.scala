package com.atguigu.bigdata.spark.core.wc

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

    //TODO 关闭连接
    sc.stop()

  }
}

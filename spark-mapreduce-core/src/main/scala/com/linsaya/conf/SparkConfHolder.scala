package com.linsaya.conf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

abstract class SparkConfHolder(application: String) {

  val conf = new SparkConf()
//    .setAppName(application)
  //    .setMaster("yarn-client")
  //    .set("spark.yarn.queue", "production")
  //    .set("spark.executor.instances", "6") //num-executors数量
  //    .set("spark.executor.memory", "2g") //executor.memory 额外的执行器内存
  //    .set("spark.executor.cores", "2") // num-executors * executor-cores 不能超过集群cpu数量
  //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  //    .set("spark.default.parallelism", "200")
  //    .set("spark.sql.shuffle.partitions", "500")
  val sc = new SparkContext(conf)
  val hdc = sc.hadoopConfiguration
  val hdfs = org.apache.hadoop.fs.FileSystem.get(hdc)
  val sqlContext = new SQLContext(sc)
  val hiveCtx = new HiveContext(sc)
  sqlContext.sql("SET spark.sql.shuffle.partitions=100")

}

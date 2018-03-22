package com.linsaya.conf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


abstract class SparkConfHolder(application: String) {

  val conf = new SparkConf()
  val sc = new SparkContext(conf)
  val hdc = sc.hadoopConfiguration
  //  val hdfs = org.apache.hadoop.fs.FileSystem.get(hdc)
  val sqlContext = new SQLContext(sc)
  val hiveCtx = new HiveContext(sc)
  hiveCtx.sql("SET spark.sql.shuffle.partitions=100")

}

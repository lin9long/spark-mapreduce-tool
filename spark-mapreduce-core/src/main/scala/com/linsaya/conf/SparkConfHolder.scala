package com.linsaya.conf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


class SparkConfHolder {

  var conf: SparkConf = new SparkConf()
  var sc: SparkContext = new SparkContext(conf)
  //  val hdc = sc.hadoopConfiguration
  //  val hdfs = org.apache.hadoop.fs.FileSystem.get(hdc)
  var sqlContext: SQLContext = new SQLContext(sc)
  var hiveCtx: HiveContext = new HiveContext(sc)
  sqlContext.sql("SET spark.sql.shuffle.partitions=100")

}

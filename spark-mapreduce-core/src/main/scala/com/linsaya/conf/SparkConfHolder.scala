package com.linsaya.conf

import com.linsaya.common.util.LoggerUtil
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


trait SparkConfHolder extends LoggerUtil {

  var conf: SparkConf = _
  if (conf == null) conf = new SparkConf()
  var sc: SparkContext = _
  if (sc == null) info("SparkContext is null, new a SparkContext")
  sc = new SparkContext(conf)
  //  val hdc = sc.hadoopConfiguration
  //  val hdfs = org.apache.hadoop.fs.FileSystem.get(hdc)
  var sqlContext: SQLContext = _
  if (sqlContext == null) sqlContext = new SQLContext(sc)
  var hiveCtx: HiveContext = _
  if (hiveCtx == null) hiveCtx = new HiveContext(sc)
  hiveCtx.sql("SET spark.sql.shuffle.partitions=100")

}

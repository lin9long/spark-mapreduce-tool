package com.richstone.mintaka.gemstack.job

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

abstract trait MapReduceJob {
  def excuteJob(sc:SparkContext,hiveCtx:HiveContext,sqlContext:SQLContext)
}

package com.linsaya.job.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.job.MapReduceJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

class StatisticsMapReduceJob extends MapReduceJob{
  override def excuteJob(sc: SparkContext,
                         hiveCtx: HiveContext,
                         sqlContext: SQLContext): Unit = {
    println("excuteJob------->StatisticsMapReduceJob")
  }
}

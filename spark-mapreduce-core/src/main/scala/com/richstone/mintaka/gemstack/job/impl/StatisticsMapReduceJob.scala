package com.richstone.mintaka.gemstack.job.impl

import com.richstone.mintaka.gemstack.SparkStatisticsJob
import com.richstone.mintaka.gemstack.job.MapReduceJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @Description: 数据统计逻辑
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
class StatisticsMapReduceJob extends MapReduceJob{
  override def excuteJob(sc: SparkContext,
                         hiveCtx: HiveContext,
                         sqlContext: SQLContext): Unit = {
    println("excuteJob------->StatisticsMapReduceJob")
  }
}

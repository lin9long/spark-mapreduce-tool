package com.linsaya.worker

import com.linsaya.SparkStatisticsJob
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2710:26
  */
trait StatisticeWorker {
  def excuteStatistics(sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext)
}

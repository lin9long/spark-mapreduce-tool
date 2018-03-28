package com.richstone.mintaka.gemstack.worker

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2710:26
  */
trait StatisticeWorker {
  def excuteStatistics(sqlContext: SQLContext, hiveCtx: HiveContext)
}

package com.richstone.mintaka.gempile.dg

import com.richstone.mintaka.gempile.dg.mapreduce.StatisticsBatchJob
import com.richstone.mintaka.gempile.dg.mapreduce.impl.KpiStatisticMapreduceJob
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.manager.ApplicationContextManager


/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/211:22
  */
object SparkBatchJob extends LoggerUtil with ApplicationContextManager {


  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      error("main args must more then 8")
      System.exit(0)
    }
    val clazzName = args(0)
    excuteJob(clazzName)
  }

  def excuteJob(clazzName: String): Unit = {
    getBean[KpiStatisticMapreduceJob](clazzName).excuteJob()
  }

}

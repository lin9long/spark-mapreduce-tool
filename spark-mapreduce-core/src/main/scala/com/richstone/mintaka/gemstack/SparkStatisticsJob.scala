package com.richstone.mintaka.gemstack

import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.job.MapReduceJob
import com.richstone.mintaka.gemstack.manager.ApplicationContextManager

/**
  * @deprecated spark统计入库
  *
  * @author llz
  * @create 2018-03-17 15:16
  **/
object SparkStatisticsJob extends LoggerUtil with ApplicationContextManager {

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      error("main args must with executor className")
      System.exit(0)
    }
    val className = args(0)
    val appConf = System.getProperty("appConf.path")
    if (appConf.isEmpty) {
      error("appConf name is not set")
      System.exit(0)
    }
    excuteJob(className)
  }

  /**
    * @Description: 执行统计入口方法
    * @param: [className]
    * @return: void
    * @author: llz
    * @Date: 2018/3/28
    */
  def excuteJob(className: String): Unit = {
    getBean[MapReduceJob](className).excuteJob()
  }

}

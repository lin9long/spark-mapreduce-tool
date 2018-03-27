package com.richstone.mintaka.gemstack

import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.conf.SparkConfHolder
import com.richstone.mintaka.gemstack.manager.PropFileManager
import org.apache.spark.sql.SQLContext
import com.richstone.mintaka.gemstack.common.Constants
import com.richstone.mintaka.gemstack.job.MapReduceJob

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 15:16
  **/
object SparkStatisticsJob extends SparkConfHolder
  with PropFileManager with Constants with LoggerUtil {

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
    val appPropFile = getPropertiesFile(appConf)

    excuteJob(className)

  }

  def excuteJob(className: String): Unit = {
    val clazz = Class.forName(className)
    clazz.newInstance().asInstanceOf[MapReduceJob].excuteJob(sc,hiveCtx,sqlContext)
  }

}

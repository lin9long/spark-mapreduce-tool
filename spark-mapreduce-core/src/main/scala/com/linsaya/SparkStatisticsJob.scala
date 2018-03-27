package com.linsaya

import com.linsaya.common.Constants
import com.linsaya.common.util.LoggerUtil
import com.linsaya.conf.SparkConfHolder
import com.linsaya.job.MapReduceJob
import com.linsaya.manager.PropFileManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

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

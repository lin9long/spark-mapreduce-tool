package com.linsaya

import com.linsaya.common.Constants
import com.linsaya.common.util.LoggerUtil
import com.linsaya.conf.SparkConfHolder
import com.linsaya.job.MapReduceJob
import com.linsaya.manager.{DataSourcePropManager, KpiStatisticsPropManager, PropFileManager, RDBPropManager}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 15:16
  **/
object SparkStatisticsJob extends SparkConfHolder(application = System.getProperty("appname"))
  with PropFileManager with RDBPropManager with KpiStatisticsPropManager
  with DataSourcePropManager with Constants with LoggerUtil {


  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      error("main args must with executor className")
    }
    val className = args(0)
    val slicetime = System.getProperty("slicetime")
    val appConf = System.getProperty("appConf.path")
    if (appConf.isEmpty) error("appConf name is not set")
    val appPropFile = getPropertiesFile(appConf)

    val kpipaths = appPropFile.getProperty(kpi_statistics_sql_file_paths).split(",")
    val datasourcepaths = appPropFile.getProperty(hive_data_source_sql_file_paths).split(",")
    val rdbsourcepaths = appPropFile.getProperty(rdb_data_source_sql_file_paths).split(",")

    val kpiStatisticsProps = genKpiStatisticsProp(kpipaths)
    val dataSourceProps = genDataSourceSQLProp(datasourcepaths)
    val RDBprops = genRDBSQLProp(rdbsourcepaths)

    excuteJob(className, kpiStatisticsProps, dataSourceProps, RDBprops)
    sqlContext.sql("select * from teacher_tmp").show()
    sqlContext.sql("select * from student_tmp").show()

  }

  def excuteJob(className: String, kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
                dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp]): Unit = {
    val clazz = Class.forName(className)
    //    if (clazz.getClass == classOf[MapReduceJob]) {
    clazz.newInstance().asInstanceOf[MapReduceJob].excuteJob(sc, hiveCtx, sqlContext, kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp],
      dataSourceProps: IndexedSeq[SparkStatisticsJob.DataSourceSQLProp], RDBprops: IndexedSeq[SparkStatisticsJob.RDBSQLProp])
    //    }
  }

}

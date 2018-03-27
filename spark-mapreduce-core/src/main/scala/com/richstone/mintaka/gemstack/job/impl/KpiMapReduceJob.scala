package com.richstone.mintaka.gemstack.job.impl

import com.richstone.mintaka.gemstack.job.MapReduceJob
import com.richstone.mintaka.gemstack.manager.{HiveSourcePropManager, KpiStatisticsPropManager, RDBPropManager}
import com.richstone.mintaka.gemstack.reader.impl.{HiveDataSourceReader, RDBSourceReader}
import com.richstone.mintaka.gemstack.worker.StatisticeWorker
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2717:34
  */
class KpiMapReduceJob extends MapReduceJob
  with HiveSourcePropManager with RDBPropManager with KpiStatisticsPropManager{

  override def excuteJob(sc: SparkContext,
                         hiveCtx: HiveContext,
                         sqlContext: SQLContext): Unit = {

    val hiveSourceProps = genDataSourceSQLProp(getSysPropertiesFile)
    if (!hiveSourceProps.isEmpty) {
      info(s"hiveSQLProps size is not empty ,size is ${hiveSourceProps.length}")
      val clazz = Class.forName("com.richstone.mintaka.gemstack.reader.impl.HiveDataSourceReader")
      clazz.newInstance().
        asInstanceOf[HiveDataSourceReader].readDataSource(sc, sqlContext, hiveCtx)
    }

    val rdbSQLProps =genRDBSQLProp(getSysPropertiesFile)
    if (!rdbSQLProps.isEmpty) {
      info(s"rdbSQLProps size is not empty ,size is ${rdbSQLProps.length}")
      val clazz = Class.forName("com.richstone.mintaka.gemstack.reader.impl.RDBSourceReader")
      clazz.newInstance().
        asInstanceOf[RDBSourceReader].readDataSource(sc, sqlContext, hiveCtx)
    }

    val kpiStatisticsProps = genKpiStatisticsProp(getSysPropertiesFile)
    if (!kpiStatisticsProps.isEmpty) {
      info(s"kpiStatisticsProps size is not empty ,size is ${kpiStatisticsProps.length}")
      val clazz = Class.forName("com.richstone.mintaka.gemstack.worker.impl.KpiStatisticsWorker")
      clazz.newInstance().
        asInstanceOf[StatisticeWorker].excuteStatistics(sc, sqlContext, hiveCtx)
    }
  }
}

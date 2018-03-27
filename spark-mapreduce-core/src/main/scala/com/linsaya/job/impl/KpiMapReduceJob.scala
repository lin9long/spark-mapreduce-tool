package com.linsaya.job.impl
import com.linsaya.job.MapReduceJob
import com.linsaya.manager.{HiveSourcePropManager, KpiStatisticsPropManager, RDBPropManager}
import com.linsaya.reader.impl.{HiveDataSourceReader, RDBSourceReader}
import com.linsaya.worker.StatisticeWorker
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

class KpiMapReduceJob extends MapReduceJob
  with HiveSourcePropManager with RDBPropManager with KpiStatisticsPropManager{

  override def excuteJob(sc: SparkContext,
                         hiveCtx: HiveContext,
                         sqlContext: SQLContext): Unit = {

    val hiveSourceProps = genDataSourceSQLProp(getSysPropertiesFile)
    if (!hiveSourceProps.isEmpty) {
      info(s"hiveSQLProps size is not empty ,size is ${hiveSourceProps.length}")
      val clazz = Class.forName("com.linsaya.reader.impl.HiveDataSourceReader")
      clazz.newInstance().
        asInstanceOf[HiveDataSourceReader].readDataSource(sc, sqlContext, hiveCtx)
    }

    val rdbSQLProps =genRDBSQLProp(getSysPropertiesFile)
    if (!rdbSQLProps.isEmpty) {
      info(s"rdbSQLProps size is not empty ,size is ${rdbSQLProps.length}")
      val clazz = Class.forName("com.linsaya.reader.impl.RDBSourceReader")
      clazz.newInstance().
        asInstanceOf[RDBSourceReader].readDataSource(sc, sqlContext, hiveCtx)
    }

    val kpiStatisticsProps = genKpiStatisticsProp(getSysPropertiesFile)
    if (!kpiStatisticsProps.isEmpty) {
      info(s"kpiStatisticsProps size is not empty ,size is ${kpiStatisticsProps.length}")
      val clazz = Class.forName("com.linsaya.worker.impl.KpiStatisticsWorker")
      clazz.newInstance().
        asInstanceOf[StatisticeWorker].excuteStatistics(sc, sqlContext, hiveCtx)
    }
  }
}

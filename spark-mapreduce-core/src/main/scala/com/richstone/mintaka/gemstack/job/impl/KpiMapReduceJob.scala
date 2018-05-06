package com.richstone.mintaka.gemstack.job.impl

import com.richstone.mintaka.gemstack.job.MapReduceJob
import com.richstone.mintaka.gemstack.manager._
import com.richstone.mintaka.gemstack.reader.SourceReader
import com.richstone.mintaka.gemstack.reader.impl.{HiveDataSourceReader, RdbSourceReader}
import com.richstone.mintaka.gemstack.worker.StatisticeWorker

/**
  * @Description: kpi统计逻辑
  * @author llz
  * @date 2018/3/2717:34
  */
@SerialVersionUID(9146747180743153650L)
class KpiMapReduceJob extends MapReduceJob with HardxdrPropManager
  with HiveSourcePropManager with RdbSourcePropManager
  with KpiStatisticsPropManager with ApplicationContextManager
  with Serializable {

  override def excuteJob(): Unit = {
    val hardxdrProps = genHardxdrProp(getSysPropertiesFile)
    if (hardxdrProps != null) if (!hardxdrProps.isEmpty) {
      info(s"hardxdrProps is not empty,size is ${hardxdrProps.size}")
      for (prop <- hardxdrProps) info(s"hardxdrProps sql no is ${prop.sqlno}")
      getBean[HardXdrEtlReader]("com.richstone.mintaka.gemstack.reader.impl.HardXdrEtlReader")
            .readDataSource(sqlContext,hiveCtx,hardxdrProps)
    }


    val hiveSourceProps = genDataSourceSQLProp(getSysPropertiesFile)
    if (hiveSourceProps != null) if (!hiveSourceProps.isEmpty) {
      info(s"hiveSQLProps size is not empty ,size is ${hiveSourceProps.length}")
      getBean[HiveDataSourceReader]("com.richstone.mintaka.gemstack.reader.impl.HiveDataSourceReader")
        .readDataSource(sqlContext, hiveCtx, hiveSourceProps)
    }

    val rdbSQLProps = genRDBSQLProp(getSysPropertiesFile)
    if (rdbSQLProps != null) if (!rdbSQLProps.isEmpty) {
      info(s"rdbSQLProps size is not empty ,size is ${rdbSQLProps.length}")
      getBean[RdbSourceReader]("com.richstone.mintaka.gemstack.reader.impl.RdbSourceReader")
        .readDataSource(sqlContext, hiveCtx, rdbSQLProps)

    }

    val kpiStatisticsProps = genKpiStatisticsProp(getSysPropertiesFile)
    if (kpiStatisticsProps != null) if (!kpiStatisticsProps.isEmpty) {
      info(s"kpiStatisticsProps size is not empty ,size is ${kpiStatisticsProps.length}")
      getBean[StatisticeWorker]("com.richstone.mintaka.gemstack.worker.impl.KpiStatisticsWorker")
        .excuteStatistics(sqlContext, hiveCtx)
    }
  }
}

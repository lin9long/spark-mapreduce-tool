package com.linsaya.worker.impl

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.{LoggerUtil, SaveTableUtils}
import com.linsaya.manager.KpiStatisticsPropManager
import com.linsaya.worker.StatisticeWorker
import com.linsaya.writer.{DataFrameHdfsWriter, DataframeRdbWriter}
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 11:31
  **/
class KpiStatisticsWorker extends StatisticeWorker with KpiStatisticsPropManager
  with SaveTableUtils with DataframeRdbWriter with DataFrameHdfsWriter{

  def excuteStatistics(sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext) = {
    var dataframe: DataFrame = null
    val kpiStatisticsProps = genKpiStatisticsProp(getSysPropertiesFile)
    for (kpiProp <- kpiStatisticsProps) {
      if (!kpiProp.sql.isEmpty) {
        info(s"KpiStatistics sql is ${kpiProp.sql},sqlNo is ${kpiProp.sqlNo}")
        dataframe = hiveCtx.sql(kpiProp.sql)
      }
      if (kpiProp.needCacheTable == "Y" && !kpiProp.storageLevel.isEmpty) {
        info(s"KpiStatistics storageLevel is ${kpiProp.storageLevel},sqlNo is ${kpiProp.sqlNo}")
        dataframe.persist(getStorageLevel(kpiProp.storageLevel))
      }
      if (!kpiProp.targetPathOfHDFS.isEmpty) {
        val path = replacePlaceholder(kpiProp.targetPathOfHDFS)
        info(s"KpiStatistics targetPathInHdfs is ${path}")
        writeDataFrameToHdfs(path,dataframe)
//        dataframe.write.mode(SaveMode.Append).parquet(replacePlaceholder(kpiProp.targetPathOfHDFS))
      }
      if (!kpiProp.targetTableNameInDB.isEmpty) {
        info(s"KpiStatistics targetTableNameInDB is ${replacePlaceholder(kpiProp.targetTableNameInDB)}")
        writeDataFrameToRdb("local-oracle",kpiProp.targetTableNameInDB, dataframe)
      }
    }
  }
}

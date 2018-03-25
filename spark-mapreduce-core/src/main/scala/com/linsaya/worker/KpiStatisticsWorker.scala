package com.linsaya.worker

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.{SaveTableUtils, LoggerUtil}
import com.linsaya.writer.DataframeHbaseWriter
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 11:31
  **/
trait KpiStatisticsWorker extends LoggerUtil with SaveTableUtils with DataframeHbaseWriter {

  def excuteKpiStatistics(hiveCtx: SQLContext, kpiStatisticsProps: IndexedSeq[SparkStatisticsJob.KpiStatisticsSQLProp]) = {
    var dataframe: DataFrame = null
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
        info(s"targetPathInHdfs is ${replacePlaceholder(kpiProp.targetPathOfHDFS)}")
        dataframe.write.parquet(replacePlaceholder(kpiProp.targetPathOfHDFS))
      }
      if (!kpiProp.targetTableNameInDB.isEmpty) {
        info(s"targetTableNameInDB is ${replacePlaceholder(kpiProp.targetTableNameInDB)}")
        writeDataFrameToHbase(kpiProp.targetTableNameInDB, dataframe)
      }
    }
  }
}

package com.linsaya.worker

import com.linsaya.SparkStatisticsJob
import com.linsaya.common.util.{CacheTableUtils, LoggerUtil}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 11:31
  **/
trait KpiStatisticsWorker extends LoggerUtil with CacheTableUtils {

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
        dataframe.write.mode(SaveMode.Overwrite).parquet(replacePlaceholder(kpiProp.targetPathOfHDFS))
      }
    }
  }
}

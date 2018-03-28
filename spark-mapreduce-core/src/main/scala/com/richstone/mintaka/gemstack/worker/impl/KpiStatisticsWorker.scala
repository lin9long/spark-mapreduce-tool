package com.richstone.mintaka.gemstack.worker.impl

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.common.util.SaveTableUtils
import com.richstone.mintaka.gemstack.manager.KpiStatisticsPropManager
import com.richstone.mintaka.gemstack.worker.StatisticeWorker
import com.richstone.mintaka.gemstack.writer.{DataFrameHdfsWriter, DataframeRdbWriter}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/3/2717:31
  */
class KpiStatisticsWorker extends StatisticeWorker with KpiStatisticsPropManager
  with SaveTableUtils with DataframeRdbWriter with DataFrameHdfsWriter{

  def excuteStatistics( sqlContext: SQLContext, hiveCtx: HiveContext) = {
    var dataframe: DataFrame = null
    val kpiStatisticsProps = genKpiStatisticsProp(getSysPropertiesFile)
    for (kpiProp <- kpiStatisticsProps) {
      if (!kpiProp.sql.isEmpty) {
        info(s"KpiStatistics sql is ${kpiProp.sql},sqlNo is ${kpiProp.sqlNo}")
        dataframe = hiveCtx.sql(kpiProp.sql)
      }
      //自定义转换
      if (!kpiProp.customTransformBeanName.isEmpty) {
        val className = kpiProp.customTransformBeanName
        val clazz = Class.forName(className)
        info(s"dataframe customTransForm model is ${kpiProp.customTransformBeanName}")
        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sqlContext.sparkContext)
      }
      //是否需要缓存
      if (kpiProp.needCacheTable == "Y" && !kpiProp.storageLevel.isEmpty) {
        info(s"KpiStatistics storageLevel is ${kpiProp.storageLevel},sqlNo is ${kpiProp.sqlNo}")
        dataframe.persist(getStorageLevel(kpiProp.storageLevel))
      }
      //是否需要保存到hdfs
      if (!kpiProp.targetPathOfHDFS.isEmpty) {
        val path = replacePlaceholder(kpiProp.targetPathOfHDFS)
        info(s"KpiStatistics targetPathInHdfs is ${path}")
        writeDataFrameToHdfs(path,dataframe)
        //        dataframe.write.mode(SaveMode.Append).parquet(replacePlaceholder(kpiProp.targetPathOfHDFS))
      }
      //是否需要入到指定的数据库
      if (!kpiProp.targetTableNameInDB.isEmpty) {
        info(s"KpiStatistics targetTableNameInDB is ${replacePlaceholder(kpiProp.targetTableNameInDB)}")
        writeDataFrameToRdb("local-oracle",kpiProp.targetTableNameInDB, dataframe)
      }
    }
  }
}

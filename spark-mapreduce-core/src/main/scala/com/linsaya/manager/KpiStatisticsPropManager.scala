package com.linsaya.manager

import com.linsaya.SparkStatisticsJob.getPropertiesFile
import com.linsaya.common.util.LoggerUtil
import com.linsaya.common.util.LoggerUtil

trait KpiStatisticsPropManager extends LoggerUtil {

  case class KpiStatisticsSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                                  targetTableNameInDB: String, targetPathInHdfs: String, tmpTableNameInSpark: String)

  def genKpiStatisticsProp(paths: Array[String]): IndexedSeq[KpiStatisticsSQLProp] = {
    if (paths.isEmpty){
      error("KpiStatisticsSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.sortBy(p => p.getProperty("sqlNo"))
    val propList = for (prop <- propertieses) yield (new KpiStatisticsSQLProp(prop.getProperty("sqlNo",""),
      prop.getProperty("sql",""), prop.getProperty("storageLevel",""), prop.getProperty("needCacheTable",""),
      prop.getProperty("targetTableNameInDB",""), prop.getProperty("targetPathInHdfs",""),
      prop.getProperty("tmpTableNameInSpark","")
    ))
    propList.seq
  }
}

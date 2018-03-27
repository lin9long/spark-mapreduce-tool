package com.linsaya.manager

import java.util.Properties

import com.linsaya.SparkStatisticsJob.{getPropertiesFile, kpi_statistics_sql_file_paths}
import com.linsaya.common.util.LoggerUtil

trait KpiStatisticsPropManager extends LoggerUtil with PropFileManager {

  case class KpiStatisticsSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                                  targetTableNameInDB: String, targetPathOfHDFS: String, tmpTableNameInSpark: String)

  def genKpiStatisticsProp(appPropFile: Properties): IndexedSeq[KpiStatisticsSQLProp] = {
    val paths = appPropFile.getProperty(kpi_statistics_sql_file_paths).split(",")
    if (paths.isEmpty){
      error("KpiStatisticsSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.sortBy(p => p.getProperty("sqlNo"))
    val propList = for (prop <- propertieses) yield (new KpiStatisticsSQLProp(prop.getProperty("sqlNo",""),
      prop.getProperty("sql",""), prop.getProperty("storageLevel",""), prop.getProperty("needCacheTable",""),
      prop.getProperty("targetTableNameInDB",""), prop.getProperty("targetPathOfHDFS",""),
      prop.getProperty("tmpTableNameInSpark","")
    ))
    propList.seq
  }
}

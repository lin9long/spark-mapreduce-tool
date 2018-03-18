package com.linsaya.manager

import com.linsaya.SparkStatisticsJob.getPropertiesFile
import com.linsaya.common.util.LoggerUtil
import com.linsaya.common.util.LoggerUtil

trait DataSourcePropManager extends LoggerUtil {

  case class DataSourceSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                               targetTableNameInDB: String, tmpTableNameInSpark: String)

  def genDataSourceSQLProp(paths: Array[String]): IndexedSeq[DataSourceSQLProp] = {
    if (paths.isEmpty) {
      error("DataSourceSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.sortBy(p => p.getProperty("sqlNo"))
    val propList = for (prop <- propertieses) yield (new DataSourceSQLProp(prop.getProperty("sqlNo", ""),
      prop.getProperty("sql", ""), prop.getProperty("storageLevel", ""), prop.getProperty("needCacheTable", ""),
      prop.getProperty("targetTableNameInDB", ""), prop.getProperty("tmpTableNameInSpark", "")
    ))
    propList.seq
  }
}

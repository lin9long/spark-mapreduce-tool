package com.linsaya.manager

import java.util.Properties

import com.linsaya.SparkStatisticsJob.{getPropertiesFile, hive_data_source_sql_file_paths}
import com.linsaya.common.util.LoggerUtil

trait HiveSourcePropManager extends LoggerUtil with PropFileManager{

  def genDataSourceSQLProp(appPropFile:Properties): IndexedSeq[DataSourceSQLProp] = {
    val paths = appPropFile.getProperty(hive_data_source_sql_file_paths).split(",")
    if (paths.isEmpty) {
      error("DataSourceSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.sortBy(p => p.getProperty("sqlNo"))
    val propList = for (prop <- propertieses) yield (new DataSourceSQLProp(prop.getProperty("sqlNo", ""),
      prop.getProperty("sql", ""), prop.getProperty("storageLevel", ""), prop.getProperty("needCacheTable", ""),
      prop.getProperty("targetTableNameInDB", ""), prop.getProperty("tmpTableNameInSpark", ""),
      prop.getProperty("sourceTableName", "")
    ))
    propList.seq
  }

  case class DataSourceSQLProp(sqlNo: String, sql: String, storageLevel: String, needCacheTable: String,
                               targetTableNameInDB: String, tmpTableNameInSpark: String,
                               sourceTableName: String)

}

package com.richstone.mintaka.gemstack.manager

import java.util.Properties

import com.richstone.mintaka.gemstack.common.util.LoggerUtil

/**
  * @Description: hive数据源管理类
  * @param:
  * @return:
  * @author: llz
  * @Date: 2018/3/28
  */
trait HiveSourcePropManager extends LoggerUtil with PropFileManager with CaseClassManager {

  def genDataSourceSQLProp(appPropFile: Properties): IndexedSeq[DataSourceSQLProp] = {
    val paths = if (appPropFile.getProperty(hive_data_source_sql_file_paths).equals("")) return null
    else appPropFile.getProperty(hive_data_source_sql_file_paths).split(",")
    if (paths.isEmpty) {
      error("DataSourceSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
    val propertieses = props.filter(_!=null).sortBy(p => Integer.valueOf(p.getProperty("sqlNo")))
    val propList = for (prop <- propertieses) yield new DataSourceSQLProp(prop.getProperty("sqlNo", ""),
      prop.getProperty("sql", ""), prop.getProperty("storageLevel", ""), prop.getProperty("needCacheTable", ""),
      prop.getProperty("targetTableNameInDB", ""), prop.getProperty("tmpTableNameInSpark", ""),
      prop.getProperty("sourceTableName", ""), prop.getProperty("customTransformBeanName", "")
    )

    propList.seq
  }


}

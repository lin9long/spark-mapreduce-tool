package com.richstone.mintaka.gemstack.common.util

import com.richstone.mintaka.gemstack.manager.PropFileManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel

/**
  * @Description 保存表工具类
  * @author llz
  * @create 2018-03-25 11:41
  **/
trait SaveTableUtil extends PropFileManager {

  def getStorageLevel(str: String): StorageLevel = {
    str match {
      case "MEMORY_AND_DISK" => StorageLevel.MEMORY_AND_DISK
      case "MEMORY_ONLY" => StorageLevel.MEMORY_ONLY
      case "DISK_ONLY" => StorageLevel.DISK_ONLY
      case "MEMORY_ONLY_SER" => StorageLevel.MEMORY_ONLY_SER
      case "MEMORY_AND_DISK_SER" => StorageLevel.MEMORY_AND_DISK_SER
      case _ => StorageLevel.DISK_ONLY
    }
  }

  def registerTempTable(tableName: String, df: DataFrame, hiveCtx: HiveContext, storageLevel: String) = {
    if (tableName.isEmpty) error("tableName is null") else {
      info(s"register temp table name is $tableName")
      df.registerTempTable(tableName)
      if (!storageLevel.isEmpty) {
        info(s"$tableName persist storageLevel is $storageLevel")
        df.persist(getStorageLevel(storageLevel))
      }
    }
  }

  def replacePlaceholder(str: String): String = {
    var newstr = str
    val pattern = "\\$\\{([\\w]+)\\}".r
    val values = pattern.findAllIn(str).map(getPlaceholderValue)
    values.foreach(tuple => {
      newstr = newstr.replace("${" + tuple._1 + "}", tuple._2)
    })
    newstr
  }


  def getPlaceholderValue(str: String): (String, String) = {
    val key = str.substring(2, str.length - 1)
    val value = getProperty(key)
    (key, value)
  }
}

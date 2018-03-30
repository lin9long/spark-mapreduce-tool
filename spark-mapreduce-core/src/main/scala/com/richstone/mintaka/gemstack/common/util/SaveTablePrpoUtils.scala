package com.richstone.mintaka.gemstack.common.util

import com.richstone.mintaka.gemstack.manager.PropFileManager
import org.apache.spark.storage.StorageLevel

/**
  * @Description 保存表工具类
  *
  * @author llz
  * @create 2018-03-25 11:41
  **/
trait SaveTablePrpoUtils extends PropFileManager{

  /**
    * @Description: 根据配置信息获取StorageLevel
    * @param: [str]
    * @return: org.apache.spark.storage.StorageLevel
    * @author: llz
    * @Date: 2018/3/29
    */
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

  /**
    * @Description: 替换占位符
    * @param: [str]
    * @return: java.lang.String
    * @author: llz
    * @Date: 2018/3/29
    */
  def replacePlaceholder(str: String): String = {
    var newstr = str
    val pattern = "\\$\\{([\\w]+)\\}".r
    val values = pattern.findAllIn(str).map(str => {
      val str2 = str.substring(2, str.length - 1)
      getPlaceholderValue(str2)})
    values.foreach(tuple => {
      newstr = newstr.replace("${"+tuple._1+"}", tuple._2)
    })
    newstr
  }

  /**
    * @Description: 根据key查找配置信息
    * @param: [key]
    * @return: scala.Tuple2<java.lang.String,java.lang.String>
    * @author: llz
    * @Date: 2018/3/29
    */
  def getPlaceholderValue(key: String): Tuple2[String, String] = {
    var value = ""
    if (!System.getProperty(key).isEmpty) {
      value = System.getProperty(key)
    } else {
      val appConf = System.getProperty("appConf.path")
      if (appConf.isEmpty) error("appConf name is not set")
      val appPropFile = getPropertiesFile(appConf)
      if (!appPropFile.getProperty(key).isEmpty) {
        value = appPropFile.getProperty(key)
      }
    }
    (key, value)
  }
}

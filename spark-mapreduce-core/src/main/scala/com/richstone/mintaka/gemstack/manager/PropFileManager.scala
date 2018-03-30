package com.richstone.mintaka.gemstack.manager

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties

import com.richstone.mintaka.gemstack.common.util.LoggerUtil


trait PropFileManager extends LoggerUtil {

  /**
    * @Description: 获取配置文件，转化为Properties
    * @param: [filepath]
    * @return: _root_.java.util.Properties
    * @author: llz
    * @Date: 2018/3/25
    */
  def getPropertiesFile(filepath: String): Properties = {
    val ips = new BufferedInputStream(new FileInputStream(filepath))
    val prop = new Properties
    prop.load(ips)
    prop
  }

  def getProperty(key: String): String = {
    val jvmProp = System.getProperties
    val value = if (getSysPropertiesFile.getProperty(key).isEmpty)
      jvmProp.getProperty(key) else getSysPropertiesFile.getProperty(key)
    value
  }

  def getSysPropertiesFile: Properties = {
    if (System.getProperty("appConf.path").isEmpty)
      error("appConf path is empty,please set appConf file")
    getPropertiesFile(System.getProperty("appConf.path"))
  }

  def getDefaultPartition: String = {
    getSysPropertiesFile.getProperty("defaultPartition", "3")
  }

  def getDefaultJdbcUrl: String = {
    getSysPropertiesFile.getProperty("jdbc.url", "")
  }

  def getDefaultJdbcUser: String = {
    getSysPropertiesFile.getProperty("jdbc.user", "")
  }

  def getDefaultJdbcDriver: String = {
    getSysPropertiesFile.getProperty("jdbc.driver", "")
  }

  def getDefaultJdbcPassWord: String = {
    getSysPropertiesFile.getProperty("jdbc.password", "")
  }

}

package com.richstone.mintaka.gemstack.common.util

import java.util.Properties

import com.richstone.mintaka.gemstack.manager.PropFileManager

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-25 13:15
  **/
object test extends PropFileManager{
  def main(args: Array[String]): Unit = {
    val prop = new Properties()
    val str = prop.getProperty("12323")
    println(str)
    replacePlaceholder("/richstone/mintaka/teacher/slicetime=${slicetime}")
  }

  def replacePlaceholder(str: String): String = {
    var newstr = str
    val pattern = "\\$\\{([\\w]+)\\}".r
    val values = pattern.findAllIn(str).map(str => {
      val str2 = str.substring(2, str.length - 1)
      getPlaceholderValue(str2)
    })
    values.foreach(tuple => {
      println(tuple)
      newstr = newstr.replace("${"+tuple._1+"}", tuple._2)
    })
    println(newstr)
    newstr
  }

  def getPlaceholderValue(key: String): Tuple2[String, String] = {
    var value = ""
    if (!key.isEmpty) {
      value = "20170101"
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

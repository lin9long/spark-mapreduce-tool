package com.richstone.mintaka.gempile.dg.manager

import java.util.Properties

import com.richstone.mintaka.gempile.dg.Utils.Constants
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.manager.PropFileManager

import scala.collection.mutable._
import scala.io.Source

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/415:10
  */
trait MapperManager extends LoggerUtil with PropFileManager with Constants {
  def genMapProp(appPropFile: Properties):  Map[String, Map[String, String]] = {
    val paths = if (appPropFile.getProperty(map_data_source_file_paths).equals("")) return null
    else appPropFile.getProperty(map_data_source_file_paths).split(",")
    if (paths.isEmpty) {
      error("MapperProp is empty")
    }
    info(s"MapperProp size is ${paths.length}")
    paths.foreach(println)
    val propMap = Map[String, Map[String, String]]()
    for (path <- paths) {
      val map = genKeyValueMap(path)
      propMap += (path -> map)
    }
    propMap
  }

  def genKeyValueMap(path: String): Map[String, String] = {
    info(s"Gen $path mapper files start")
    val map = Map[String, String]()
    val lines = Source.fromFile(path).getLines()
    for (line <- lines) {
//      info(s"line is $line")
      val strings = line.split(",")
      map += (strings(0) -> strings(1))
    }
    map
  }
}

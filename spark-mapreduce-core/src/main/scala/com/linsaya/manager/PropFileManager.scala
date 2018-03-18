package com.linsaya.manager

import java.io.{BufferedInputStream, File, FileInputStream}
import java.util.Properties


trait PropFileManager {

  def getPropertiesFile(filepath: String): Properties = {
      val ips = new BufferedInputStream(new FileInputStream(filepath))
      val prop = new Properties
      prop.load(ips)
      prop
  }

}

package com.linsaya.manager

import java.io.{BufferedInputStream, FileInputStream}
import java.util.Properties


trait PropFileManager {

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

}

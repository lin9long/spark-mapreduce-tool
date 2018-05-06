package com.richstone.mintaka.gempile.dg.Utils

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/217:35
  */
trait EtlUtils {

  def filterXdrData(array: Array[String], timepos: Int, length: Int): Boolean = {
    var lengthEnough = true
    if (array.length > 0)
      lengthEnough = array.length == length else lengthEnough = false
    if (lengthEnough) {
      val time = array(timepos)
      if (time.trim.length != 13 || time.isEmpty)
        lengthEnough = false else lengthEnough = true
    }
    lengthEnough
  }
}

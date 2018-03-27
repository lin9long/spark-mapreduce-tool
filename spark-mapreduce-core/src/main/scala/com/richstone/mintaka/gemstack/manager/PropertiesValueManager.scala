package com.richstone.mintaka.gemstack.manager

import java.util.Properties

import mintaka.commons.MintakaException

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 12:43
  **/
trait PropertiesValueManager {

  def getPropertValue(prop: Properties, valueName: String): String = {
    var value: String = null
    try {
      value = prop.getProperty(valueName)
    } catch {
      case e: Exception => throw new MintakaException(s"Properties do not have $valueName")
    }
    value
  }

}

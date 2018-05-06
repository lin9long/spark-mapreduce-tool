package com.richstone.mintaka.gempile.dg.manager

import java.util.Properties

import com.richstone.mintaka.gempile.dg.Utils.Constants
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import com.richstone.mintaka.gemstack.manager.PropFileManager

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/214:49
  */
trait HardxdrPropManager extends LoggerUtil with PropFileManager with Constants {

  case class HardxdrProp(xdr: String, xdrType: String, xdrInterface: String, xdrLength: String,
                         addFieldName: String, mapFieldName: String, transformedField: String, sqlno: String)

  def genHardxdrProp(appPropFile: Properties): IndexedSeq[HardxdrProp] = {
    val paths = if (appPropFile.getProperty(hardxdr_data_source_file_paths).equals("")) return null
    else appPropFile.getProperty(hardxdr_data_source_file_paths).split(",")
    if (paths.isEmpty) {
      error("DataSourceSQLProp is empty")
    }
    val props = for (i <- 0 until paths.length) yield getPropertiesFile(paths(i))
//    val propertieses = props.filter(_ != null).sortWith(_.getProperty("sqlno")>_.getProperty("sqlno"))
    val propertieses = props.filter(_ != null).sortBy(props => Integer.valueOf(props.getProperty("sqlno")))
    val propsList = for (prop <- propertieses) yield new HardxdrProp(prop.getProperty("xdr", "")
      , prop.getProperty("xdrType", ""), prop.getProperty("xdrInterface", ""), prop.getProperty("xdrLength", ""), prop.getProperty("addFieldName", "")
      , prop.getProperty("mapFieldName", ""), prop.getProperty("transformedField", ""), prop.getProperty("sqlno", ""))
    propsList
  }
}

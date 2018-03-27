package com.richstone.mintaka.gemstack.reader.impl

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.manager.HiveSourcePropManager
import com.richstone.mintaka.gemstack.reader.SourceReader
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-18 9:30
  **/
class HiveDataSourceReader extends SourceReader with HiveSourcePropManager {

  def readDataSource(sc: SparkContext, sqlContext: SQLContext, hiveCtx: HiveContext): Unit = {
    //获取hive配置文件
    val hiveProp = genDataSourceSQLProp(getSysPropertiesFile)
    for (prop <- hiveProp) {
      info(s"start load hiveSourceProp table name is ${prop.sourceTableName}")
       var dataframe: DataFrame = null
       info(s"HiveDataSourceReader sql is ${prop.sql}")
       dataframe = hiveCtx.sql(if (prop.sql.isEmpty) {
         error("please set HiveDataSourceReader sql")
         ""
       } else prop.sql)
      //自定义转换
      if (!prop.customTransformBeanName.isEmpty) {
        val className = prop.customTransformBeanName
        val clazz = Class.forName(className)
        info(s"dataframe customTransForm model is ${prop.customTransformBeanName}")
        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sc)
      }
       info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
       dataframe.registerTempTable(prop.tmpTableNameInSpark.toString)
       //      val sql = s"select * from ${prop.tmpTableNameInSpark}"
       //      info(s"execute sql is $sql")
       //      hiveCtx.sql(s"select * from ${prop.tmpTableNameInSpark}").show()
     }
   }
}

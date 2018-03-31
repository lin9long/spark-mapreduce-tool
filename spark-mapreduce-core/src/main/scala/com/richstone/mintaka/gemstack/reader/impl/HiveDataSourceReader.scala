package com.richstone.mintaka.gemstack.reader.impl

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.common.util.SaveTableUtil
import com.richstone.mintaka.gemstack.manager.CaseClassManager
import com.richstone.mintaka.gemstack.reader.SourceReader
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * hive数据源读取工具
  *
  * @author llz
  * @create 2018-03-18 9:30
  **/
class HiveDataSourceReader extends SourceReader with CaseClassManager with SaveTableUtil {


  /**
    * @Description:读取数据源
    * @param: [sc, sqlContext, hiveCtx]
    * @return: void
    * @author: llz
    * @Date: 2018/3/28
    */
  def readDataSource[A](sqlContext: SQLContext, hiveCtx: HiveContext, indexedSeq: IndexedSeq[A]): Unit = {
    //获取hive配置文件
    val hiveProp = indexedSeq.asInstanceOf[IndexedSeq[DataSourceSQLProp]]
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
        dataframe = clazz.newInstance().asInstanceOf[CustomTransform].transform(dataframe, sqlContext, sqlContext.sparkContext)
      }
      info(s"dataframe registerTempTable name is ${prop.tmpTableNameInSpark} count is ${dataframe.count()}")
      registerTempTable(prop, dataframe, hiveCtx)
    }
  }
}

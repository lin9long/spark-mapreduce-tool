package com.richstone.mintaka.gemstack.zhgd.transform.cz

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}

/**
  * 自定义类型转换器，重新组装dataframe
  *
  * @author llz
  * @create 2018-03-17 15:19
  **/
class UserTypeTransfrom extends CustomTransform with LoggerUtil with Serializable {
  override def transform(dataframe: DataFrame,
                         sqlContext: SQLContext, sc: SparkContext) = {
    //导入隐式转换
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val dfWithUserType = dataframe.withColumn("HTTP_DL_RATE", when($"app_type" isNotNull, 8888))
      .drop("statistical_time")
    val schema = dfWithUserType.schema
    val index = dfWithUserType.schema.fieldIndex("app_type")
    val structFields = DataTypes.createStructType(createSchema(schema))
    info(s"app_type index is $index newfields size is ${structFields.length} by ${schema.length}")
    //        dfWithUserType
    val rowWithHttp = dfWithUserType.map(row => {
      //生产一个包含row对象的数组
      var copyRow = for (i <- 0 until row.length) yield row.get(i)
      val value = row.getAs[String](index)
      value match {
        case "18" => copyRow = copyRow.+:(123)
        case "21" => copyRow = copyRow.+:(999)
        case _ => copyRow = copyRow.+:(1111)
      }
      //scala与javaapi不一致，java使用RowFactory.create(copyRow);
     Row.fromSeq(copyRow)
    })
    info(s"create rowWithHttpRdd is finish}")
    val transfromDf = sqlContext.createDataFrame(rowWithHttp, structFields)
    transfromDf.show()
    transfromDf
  }

  /**
    * @Description: 组装schema
    * @param: [schema]
    * @return: org.apache.spark.sql.types.StructField[]
    * @author: llz
    * @Date: 2018/3/28
    */

  def createSchema(schema: StructType): Array[StructField] = {
    var structTypes = schema.fields.clone()
    structTypes = structTypes.+:(DataTypes.createStructField("HTTP_500K_TIME_DELAY", DataTypes.IntegerType, false))
    structTypes
  }
}

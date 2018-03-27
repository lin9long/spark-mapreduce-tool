package com.richstone.mintaka.gemstack.zhgd.transform.cz

import com.richstone.mintaka.gemstack.common.CustomTransform
import com.richstone.mintaka.gemstack.common.util.LoggerUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, RowFactory, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 15:19
  **/
class UserTypeTransfrom extends CustomTransform with LoggerUtil with Serializable {
  override def transform(dataframe: DataFrame,
                         sqlContext: SQLContext, sc: SparkContext) = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val dfWithUserType = dataframe.withColumn("USER_TYPE", when($"procedure_start_time" isNotNull, "richstone"))
    val schema = dfWithUserType.schema
    val index = dfWithUserType.schema.fieldIndex("app_type")
    val fields = DataTypes.createStructType(createSchema(schema))
    info(s"app_type index is $index newfields size is ${fields.length} by ${schema.length}")
    dfWithUserType.foreach(row =>{
      val value = row.getString(index)
      info(s"app_type value is $value ")
    })
    dfWithUserType
//    val rowWithHttp = dfWithUserType.map(row => {
//      //      val rowCopy = row.copy()
//      var rowCopy = for (i <- 0 until row.length) yield row.get(i)
//      val str = row.getString(index)
//      str match {
//        case "18" => rowCopy = rowCopy +: "gemstack"
//        case "21" => rowCopy = rowCopy +: "gempile"
//        case _ =>
//      }
//     RowFactory.create(rowCopy)
//    })
//    sqlContext.createDataFrame(rowWithHttp, fields)
  }

  def createSchema(schema: StructType): Array[StructField] = {
    var structTypes = schema.fields.clone()
    DataTypes.createStructField("HTTP_DL_RATE", DataTypes.StringType, false)
    structTypes = structTypes.+:(DataTypes.createStructField("HTTP_DL_RATE", DataTypes.StringType, false))
    structTypes
  }
}

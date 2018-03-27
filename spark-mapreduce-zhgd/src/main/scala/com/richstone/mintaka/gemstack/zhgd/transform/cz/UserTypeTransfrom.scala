package com.richstone.mintaka.gemstack.zhgd.transform.cz

import com.richstone.mintaka.gemstack.common.CustomTransform
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 15:19
  **/
class UserTypeTransfrom extends CustomTransform {
  override def transform(dataframe: DataFrame, sqlContext: SQLContext, sc: SparkContext) = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    val dfWithUserType = dataframe.withColumn("USER_TYPE", when($"procedure_start_time" isNotNull, "richstone"))
//    dfWithUserType.foreach(row => {
//      val appType = row.getAs[String](4)
//    })
  }
}

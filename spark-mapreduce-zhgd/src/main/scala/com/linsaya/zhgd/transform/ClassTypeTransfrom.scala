package com.linsaya.zhgd.transform

import com.linsaya.common.CustomTransform
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 15:19
  **/
class ClassTypeTransfrom extends CustomTransform {
  override def transform(dataframe: DataFrame, sqlContext: SQLContext, sc: SparkContext) = {
    import org.apache.spark.sql.functions._
    import sqlContext.implicits._
    dataframe.withColumn("slicetime", when($"cid" isNotNull, 13312321))
  }
}

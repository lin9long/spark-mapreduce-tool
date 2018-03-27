package com.richstone.mintaka.gemstack.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * ${DESCRIPTION}
  *
  * @author llz
  * @create 2018-03-17 12:13
  **/
trait CustomTransform {
  def transform(dataframe: DataFrame, sqlContext: SQLContext, sc: SparkContext):DataFrame
}

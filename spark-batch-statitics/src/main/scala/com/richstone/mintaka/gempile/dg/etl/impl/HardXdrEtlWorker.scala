package com.richstone.mintaka.gempile.dg.etl.impl

import com.richstone.mintaka.gempile.dg.Utils.EtlUtils
import com.richstone.mintaka.gempile.dg.etl.BaseWorker
import com.richstone.mintaka.gempile.dg.manager.{HardxdrPropManager, MapperManager}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * @Description: ${todo}
  * @author llz
  * @date 2018/5/216:59
  */
class HardXdrEtlWorker extends BaseWorker with HardxdrPropManager with EtlUtils
  with MapperManager {
  override def excuteJob[A](sc: SparkContext, sqlContext: SQLContext, indexedSeq: IndexedSeq[A]): Unit = {
    val props = indexedSeq.asInstanceOf[IndexedSeq[HardxdrProp]]
    val maps = genMapProp(getSysPropertiesFile)
    for (prop <- props) {
      val xdr = prop.xdr
      val addFieldList = prop.addFieldName.split(",")
      val mapFieldList = prop.mapFieldName.split(",")
      val transformedFieldList = prop.transformedField.split(",")
      import scala.collection.mutable._
      var mapFields = Map[String, String]()
      for (mapfield <- mapFieldList) {
        mapFields += (mapfield.split(":")(0) -> mapfield.split(":")(1))
      }
      val timeposstr = mapFields.get("procedure_start_time")
      info(s"mapFields size is ${mapFields.size}")
      val field = genSchemaByList(addFieldList)
      val rawData = sc.textFile(s"$hardxdr_raw_root_path/$xdr/*.xdr")
      val xdrLength = Integer.valueOf(prop.xdrLength)
      val timepos = Integer.valueOf(timeposstr.get)
      info(s"timepos is ${Integer.valueOf(timeposstr.get)}" +
        s",xdrLength is${Integer.valueOf(prop.xdrLength)}")
      val filterData = rawData.filter(line =>
        filterXdrData(line.split(",", -1), timepos, xdrLength)
        //        line.split(",",-1).length==xdrLength
      )
      val rows = filterData.map(line => {
        val strings = line.split(",")
        //        for(map <- mapFields){
        //          val key = map._1
        //          val pos =  Integer.valueOf(map._2)
        //          if (key.eq("cell_id")){
        //            strings(pos)
        //          }
        //        }
        var values = (for {map <- mapFields} yield strings(Integer.valueOf(map._2))).toSeq
        values = values :+ "abnormal_reason"
        values = values :+ "abnormal_type"

        for (i <- transformedFieldList) {
          val key = i.split(":")(0)
          key match {
            case "sgw" =>values = values :+ "sgw"
            case "e_node_b" =>values = values :+ "e_node_b"
            case "mme" =>values = values :+ "mme"
            case "district" =>values = values :+ "district"
            case _ =>values = values :+ ""
          }
        }
        Row.fromSeq(values)
        //              Row.empty
      })
      info(s"rows.length is ${rows.take(1).length}")
      sqlContext.createDataFrame(rows, field).write.mode(SaveMode.Overwrite)
        .parquet(s"$hardxdr_root_path/$xdr/year=2017/month=5/day=16/hour=0/minute=00/")
    }
  }

  def genSchemaByList(list: Array[String]): StructType = {
    val fields = for (i <- list) yield DataTypes.createStructField(i, DataTypes.StringType, false)
    val structFields = DataTypes.createStructType(fields)
    structFields
  }


}

package com.richstone.mintaka.gemstack.writer

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.richstone.mintaka.gemstack.manager.{CaseClassManager, PropFileManager}
import mintaka.util.DataFrameConverter
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.jdbc.JdbcType
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

/**
  * @Descri ption: dataframen写入rdb数据库scala版本
  * @author llz
  * @date 2018/3/1114:41
  */
trait DataframeRdbWriter extends PropFileManager with CaseClassManager with Serializable{

  /**
    * @Description: 保存数据到RDB数据库
    * @param: [dataFrame, prop, appPropertyManager, kpiStatisticsSQL]
    * @return: void
    * @author: llz
    * @Date: 2018/3/26
    */
  def writeDataFrameToRdb(propName: String, kpiProp: KpiStatisticsSQLProp, dataFrame: DataFrame): Unit = {
    val prop = genConnProp(propName)
    val tableName = kpiProp.targetTableNameInDB
    val url = prop.getProperty("url", "")
    val rddSchema = dataFrame.schema
    info(s"dataFrame numFields is ${rddSchema.fields.length}")
    //生产插入状态的空值数据
    val nullType = dataFrame.schema.fields.map { field => getJdbcType(field.dataType).jdbcNullType }
    val start = System.currentTimeMillis()
    info(s"sql no is ${kpiProp.sqlNo} save dataframe to oracle start " +
      s"dataFrame count is ${dataFrame.count()}")
//     dataFrame = DataFrameConverter.DataFrameConverter(dataFrame)
    val partition = dataFrame.sqlContext.sparkContext.getConf.get("spark.executor.instances", getDefaultPartition).toInt
    DataFrameConverter.DataFrameConverter(dataFrame).coalesce(partition).foreachPartition(iterator => {
      val conn = JdbcUtils.createConnectionFactory(url, prop)()
      var committed = false
      val supportsTransactions = try {
        conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
          conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
      } catch {
        case NonFatal(e) =>
          warn("Exception while detecting transaction support", e)
          true
      }
      try {
        if (supportsTransactions) {
          conn.setAutoCommit(false) // Everything in the same db transaction.
        }
        //创建insertStatement，准备插入数据
        val stmt = insertStatement(conn, tableName, rddSchema)
        try {
          var rowCount = 0
          while (iterator.hasNext) {
            val row = iterator.next()
            //根据字段长度取出数据
            val numFields = rddSchema.fields.length
            var i = 0
            while (i < numFields) {
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1, nullType(i))
              } else {
                //判断类型
                rddSchema.fields(i).dataType match {
                  case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                  case LongType => stmt.setLong(i + 1, row.getLong(i))
                  case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                  case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                  case ShortType => stmt.setInt(i + 1, row.getShort(i))
                  case ByteType => stmt.setInt(i + 1, row.getByte(i))
                  case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                  case StringType => stmt.setString(i + 1, row.getString(i))
                  case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                  case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                  case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                  case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                  case ArrayType(et, _) => val array = conn.createArrayOf(
                    getJdbcType(et).databaseTypeDefinition.toLowerCase,
                    row.getSeq[AnyRef](i).toArray)
                    stmt.setArray(i + 1, array)
                  case _ => throw new IllegalArgumentException(
                    s"Can't translate non-null value for field $i")
                }
              }
              i = i + 1
            }
            stmt.addBatch()
            rowCount += 1
            if (rowCount % 10000 == 0) {
              stmt.executeBatch()
              rowCount = 0
            }
          }
          if (rowCount > 0) {
            stmt.executeBatch()
          }
        } finally {
          stmt.close()
        }
        if (supportsTransactions) {
          conn.commit()
        }
        committed = true
      } finally {
        if (!committed) {
          // The stage must fail.  We got here through an exception path, so
          // let the exception through unless rollback() or close() want to
          // tell the user about another problem.
          if (supportsTransactions) {
            conn.rollback()
          }
          conn.close()
        } else {
          // The stage must succeed.  We cannot propagate any exception close() might throw.
          try {
            conn.close()
          } catch {
            case e: Exception => warn("Transaction succeeded, but closing failed", e)
          }
        }
      }
    })
    val end = System.currentTimeMillis()
    info(s"save dataframe to oracle finish cost time is ${end - start}")

  }

  /**
    * Returns a PreparedStatement that inserts a row into table via conn.
    */
  def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    //    val placeholders = rddSchema.fields.map(field => {
    //      if (field.name.equals("statistical_time")) "to_date(?, 'yyyy-MM-dd HH24:mi:ss')" else "?"
    //    }).mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    info(s"insertStatement sql is $sql")
    conn.prepareStatement(sql)
  }

  /**
    * Retrieve standard jdbc types.
    *
    * @param dt The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
    * @return The default JdbcType for this DataType
    */
  def getCommonJDBCType(dt: DataType): Option[JdbcType] = {
    dt match {
      case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
      case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
      case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
      case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
      case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
      case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
      case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
      case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
      case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
      case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
      case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))
      case t: DecimalType => Option(
        JdbcType(s"DECIMAL(${t.precision},${t.scale})", java.sql.Types.DECIMAL))
      case _ => None
    }
  }

  /**
    * @Description: 根据dataframe类型匹配检索jdbc类型
    * @param: [dt]
    * @return: org.apache.spark.sql.jdbc.JdbcType
    * @author: llz
    * @Date: 2018/3/27
    */
  def getJdbcType(dt: DataType): JdbcType = {
    getCommonJDBCType(dt).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
    * @Description: 生成数据库连接prop
    * @param: [propName]
    * @return: java.util.Properties
    * @author: llz
    * @Date: 2018/4/2
    */
  private def genConnProp(propName: String) = {
    val connTuples = rdbProps(propName)
    val prop = new Properties()
    val sysFile = getSysPropertiesFile
    val url = sysFile.getProperty("jdbc.url", "")
    val driver = sysFile.getProperty("jdbc.driver", "")
    val user = sysFile.getProperty("jdbc.user", "")
    val password = sysFile.getProperty("jdbc.password", "")
    connTuples.foreach(tuple => prop.setProperty(tuple._1, tuple._2))
    prop
  }
}

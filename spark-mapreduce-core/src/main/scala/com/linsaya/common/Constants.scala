package com.linsaya.common

trait Constants {

  val masterUrl = "masterUrl"
  val hive_data_source_sql_file_paths = "hive.data.source.sql.file.paths"
  val rdb_data_source_sql_file_paths = "rdb.data.source.sql.file.paths"
  val kpi_statistics_sql_file_paths = "kpi.statistics.sql.file.paths"

  val rdbProps = Map(
    "mysql" -> {
      List(Tuple2("user", "root"), Tuple2("password", "root"),
        Tuple2("driver", "com.mysql.jdbc.Driver"), Tuple2("url", "jdbc:mysql://192.168.199.160:3306/school"))
    }
  )

}

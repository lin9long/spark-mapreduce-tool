package com.richstone.mintaka.gemstack.common

trait Constants {

  val masterUrl = "masterUrl"
  val hive_data_source_sql_file_paths = "hive.data.source.sql.file.paths"
  val rdb_data_source_sql_file_paths = "rdb.data.source.sql.file.paths"
  val kpi_statistics_sql_file_paths = "kpi.statistics.sql.file.paths"

  val rdbProps = Map(
    "mysql" -> {
      List(Tuple2("user", "root"),
        Tuple2("password", "root"),
        Tuple2("driver", "com.mysql.jdbc.Driver"),
        Tuple2("url", "jdbc:mysql://192.168.199.160:3306/school"))
    },
    "phoenix" -> {
      List(Tuple2("user", ""),
        Tuple2("password", ""),
        Tuple2("driver", "org.apache.phoenix.jdbc.PhoenixDriver"),
        Tuple2("url", "jdbc:phoenix:master:2181"))
    },
    "cz-oracle" -> {
      List(Tuple2("user", "CZ_HUANGYUNMEI"),
        Tuple2("password", "CZhantele888!"),
        Tuple2("driver", "oracle.jdbc.driver.OracleDriver"),
        Tuple2("url", "jdbc:oracle:thin:@//192.168.35.24:1521/CZ_HUANGYUNMEI"))
    },
    "local-oracle" -> {
      List(Tuple2("user", "CHAOZHOU"),
        Tuple2("password", "Richstone123!"),
        Tuple2("driver", "oracle.jdbc.driver.OracleDriver"),
        Tuple2("url", "jdbc:oracle:thin:@192.168.6.24:1521:xe"))
    }
  )

}

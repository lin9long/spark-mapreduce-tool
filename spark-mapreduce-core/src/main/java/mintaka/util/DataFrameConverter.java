package mintaka.util;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.spark.sql.types.DataTypes.DateType;

/**
 * @author llz
 * @Description: ${todo}
 * @date 2018/4/821:59
 */
public class DataFrameConverter {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(DataFrameConverter.class);
    public static DataFrame DataFrameConverDateType(DataFrame dataFrame){
        //获取dataframe的字段
        StructType schema = dataFrame.schema();
        String[] fieldNames = schema.fieldNames();
        //使用dataframe入库
//        LOGGER.info("Save " + targetTableNameInDB
//                + " to oracle by dataframe begin");
        List<Column> columnList = new ArrayList<>();
        //将时间字段转换为dataframe中的DateType
        for (int i = 0; i < fieldNames.length; i++) {
            Column column = new Column(fieldNames[i]);
            if (fieldNames[i].equalsIgnoreCase("STATISTICAL_TIME")
                    || fieldNames[i].equalsIgnoreCase("CREATED_TIME")) {
                column.cast(DateType);
                columnList.add(column);
                LOGGER.info("Convert " + fieldNames[i] + " cast to oracle DateType");
            } else {
                columnList.add(column);
            }
        }
        Seq<Column> columnSeq = JavaConversions.asScalaBuffer(columnList).toSeq();
        return dataFrame.select(columnSeq);
    }
}

package org.interestinglab.waterdrop.filter;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import io.github.interestinglab.waterdrop.apis.BaseFilter;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class JavaSubstring extends BaseFilter {

    private Config config;

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Tuple2<Object, String> checkConfig() {
        if (!config.hasPath("len")) {
            return new Tuple2<>(false, "please specify [len]");
        }
        return new Tuple2<>(true, "");
    }

    @Override
    public void prepare(SparkSession spark) {

        Map<String, Object> map = new HashMap();
        map.put("source_field", "raw_message");
        map.put("target_field", "__ROOT__");
        map.put("pos", 0);
        Config defaultConfig = ConfigFactory.parseMap(map);

        config = config.withFallback(defaultConfig);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> df) {
        String srcField = config.getString("source_field");
        String targetField = config.getString("target_field");

        UDF3<String, Integer, Integer, String> func = (String src, Integer pos, Integer len) -> (defineSubstring(src, pos, len));
//        UDF3 func = new UDF3<String, Integer, Integer, String>() {
//            @Override
//            public String call(String src, Integer pos, Integer len) throws Exception {
//                return defineSubstring(src, pos, len);
//            }
//        };

        int pos = this.config.getInt("pos");
        int len = this.config.getInt("len");
        spark.udf().register("func", func, DataTypes.StringType);
        return df.withColumn(targetField, callUDF("func", col(srcField), lit(pos), lit(len)));
    }

    private String defineSubstring(String src, int pos, int len) {
        return src.substring(pos, pos + len);
    }
}

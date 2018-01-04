package org.interestinglab.waterdrop.filter;

import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.substring;
import io.github.interestinglab.waterdrop.apis.BaseFilter;

import com.typesafe.config.Config;
import org.apache.spark.streaming.StreamingContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class Javasubstring extends BaseFilter {

    private Config config;

    public Javasubstring(Config config) {
        super (config);
        this.config = config;
    }

    @Override
    public Tuple2<Object, String> checkConfig() {
        if(!config.hasPath("len")) {
            return new Tuple2<>(false, "please specify [len]");
        }
        return new Tuple2<>(true, "");
    }

    @Override
    public void prepare(SparkSession spark, StreamingContext ssc) {

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
        int pos = config.getInt("pos");
        int len = config.getInt("len");
        return df.withColumn(targetField, substring(col(srcField), pos, len));
    }
}

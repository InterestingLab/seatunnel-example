package org.interestinglab.waterdrop.output;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseOutput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.StreamingContext;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class JavaStdout extends BaseOutput {

    private Config config;

    public JavaStdout(Config config) {
        super(config);
        this.config = config;
    }

    @Override
    public Tuple2<Object, String> checkConfig() {
        if (!config.hasPath("limit") || config.hasPath("limit") && config.getInt("limit") > 0) {
            return new Tuple2<>(true, "");
        } else {
            return new Tuple2<>(false, "please specify [limit] as Number");
        }
    }

    @Override
    public void prepare(SparkSession spark, StreamingContext ssc) {
        Map<String, Object> map = new HashMap();
        map.put("limit", 100);
        Config defaultConfig = ConfigFactory.parseMap(map);
        config = config.withFallback(defaultConfig);
    }

    @Override
    public void process(Dataset<Row> df) {
        int limit = config.getInt("limit");
        df.show(limit, false);
    }
}

package org.interestinglab.waterdrop.output;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.config.ConfigFactory;
import io.github.interestinglab.waterdrop.apis.BaseOutput;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

public class JavaStdout extends BaseOutput {

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
        if (!config.hasPath("limit") || config.hasPath("limit") && config.getInt("limit") > 0) {
            return new Tuple2<>(true, "");
        } else {
            return new Tuple2<>(false, "please specify [limit] as Number");
        }
    }

    @Override
    public void prepare(SparkSession spark) {
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

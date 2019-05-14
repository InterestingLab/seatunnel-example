package org.interestinglab.waterdrop.input;

import com.typesafe.config.Config;
import io.github.interestinglab.waterdrop.apis.BaseStreamingInput;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import scala.Tuple2;


public class JavaStreamingInput extends BaseStreamingInput<String> {

    private Config config;

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public Tuple2<Object, String> checkConfig() {
        return new Tuple2<>(true, "");
    }

    @Override
    public DStream<String> getDStream(StreamingContext ssc) {

//        ReceiverInputDStream<String> inputDStream = ssc.socketTextStream("hostname", 9999, StorageLevel.DISK_ONLY());
        long duration = ssc.conf().getLong("spark.streaming.batchDuration", 5);
        JavaStreamingContext jssc = new JavaStreamingContext(ssc.conf(), Durations.seconds(duration));

        JavaReceiverInputDStream<String> javaReceiverInputDStream = jssc.socketTextStream("hostname", 9999);

        return javaReceiverInputDStream.dstream();
    }


    @Override
    public Dataset<Row> rdd2dataset(SparkSession spark, RDD<String> rdd) {
        JavaRDD<Row> rows = rdd.toJavaRDD().map(row -> RowFactory.create(row));
        StructType schema = new StructType().add("raw_message", DataTypes.StringType);
        return spark.createDataFrame(rows, schema);
    }
}

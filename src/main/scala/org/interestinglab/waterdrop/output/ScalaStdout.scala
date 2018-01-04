package org.interestinglab.waterdrop.output

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class ScalaStdout(var config: Config) extends BaseOutput(config) {

  override def checkConfig(): (Boolean, String) = {
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => (true, "")
      case false => (false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "limit" -> 100
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(df: DataFrame): Unit = {
    val limit = config.getInt("limit")
    df.show(limit, false)
  }

}

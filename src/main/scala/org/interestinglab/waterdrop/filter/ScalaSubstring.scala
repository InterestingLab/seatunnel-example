package org.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.apis.BaseFilter
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{substring, col}
import org.apache.spark.streaming.StreamingContext

import scala.collection.JavaConversions._

class ScalaSubstring(var config: Config) extends BaseFilter(config) {

  def this() {
    this(ConfigFactory.empty())
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("source_field", "len")
    val nonExistsOptions:List[(String, Boolean)] = requiredOptions.map{ optionName =>
      (optionName, config.hasPath(optionName))
    }.filter { p =>
      val (optionName, exists) = p
        !exists
      }

    if (nonExistsOptions.length == 0) {
      (true, "")
    } else {
      (false, "please specify source_field as non-empty string")
    }
  }

  override def prepare(spark: SparkSession, ssc: StreamingContext): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> "__ROOT__",
        "pos" -> 0
      )
    )

    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: DataFrame): DataFrame = {

    val srcField = config.getString("source_field")
    val targetField = config.getString("target_field")
    val pos = config.getInt("pos")
    val len = config.getInt("len")
    df.withColumn(targetField, substring(col(srcField), pos, len))
  }

}

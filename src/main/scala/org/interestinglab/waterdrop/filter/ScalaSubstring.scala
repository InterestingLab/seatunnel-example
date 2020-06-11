package org.interestinglab.waterdrop.filter

import io.github.interestinglab.waterdrop.apis.BaseFilter
import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, udf, lit}

import scala.collection.JavaConversions._

class ScalaSubstring extends BaseFilter {

  var config: Config = ConfigFactory.empty()

  override def getUdfList(): List[(String, UserDefinedFunction)] = {

    val func = udf((s: String, pos: Int, len: Int) => s.substring(pos, pos+len))

    List(("my_sub", func))
  }

  /**
    * Set Config.
    **/
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
    * Get Config.
    **/
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {

    val requiredOptions = List("source_field", "len")
    val nonExistsOptions: List[(String, Boolean)] = requiredOptions.map { optionName =>
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

  override def prepare(spark: SparkSession): Unit = {

    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "source_field" -> "raw_message",
        "target_field" -> "__ROOT__",
        "pos" -> 0
      )
    )

    config = config.withFallback(defaultConfig)
  }

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {

    val srcField = config.getString("source_field")
    val targetField = config.getString("target_field")
    val pos = config.getInt("pos")
    val len = config.getInt("len")
    val func = getUdfList().get(0)._2
    df.withColumn(targetField, func(col(srcField), lit(pos), lit(len)))
  }

}

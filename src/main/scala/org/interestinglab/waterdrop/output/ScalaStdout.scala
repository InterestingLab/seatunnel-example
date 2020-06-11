package org.interestinglab.waterdrop.output

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._

class ScalaStdout extends BaseOutput {


  var config: Config = ConfigFactory.empty()

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
    !config.hasPath("limit") || (config.hasPath("limit") && config.getInt("limit") >= -1) match {
      case true => (true, "")
      case false => (false, "please specify [limit] as Number[-1, " + Int.MaxValue + "]")
    }
  }

  override def prepare(spark: SparkSession): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        "limit" -> 100
      )
    )
    config = config.withFallback(defaultConfig)
  }

  override def process(ds: Dataset[Row]): Unit = {
    val limit = config.getInt("limit")
    ds.show(limit, false)
  }

}

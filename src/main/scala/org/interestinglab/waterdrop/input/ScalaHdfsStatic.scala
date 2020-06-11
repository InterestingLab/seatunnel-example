package org.interestinglab.waterdrop.input

import io.github.interestinglab.waterdrop.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseStaticInput
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class ScalaHdfsStatic extends BaseStaticInput {

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
    config.hasPath("path") match {
      case true => {

        val dir = config.getString("path")
        val path = new org.apache.hadoop.fs.Path(dir)
        Option(path.toUri.getScheme) match {
          case None => (true, "")
          case Some(schema) => (true, "")
          case _ =>
            (
              false,
              "unsupported schema, please set the following allowed schemas: hdfs://, for example: hdfs://<name-service>:<port>/var/log")
        }
      }
      case false => (false, "please specify [path] as non-empty string")
    }
  }

  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val format = config.getString("format")
    val reader = spark.read.format(format)
    val path = config.getString("path")

    format match {
      case "text" => reader.load(path).withColumnRenamed("value", "raw_message")
      case "parquet" => reader.parquet(path)
      case "json" => reader.json(path)
      case "orc" => reader.orc(path)
      case "csv" => reader.csv(path)
      case _ => reader.load(path)
    }
  }

}

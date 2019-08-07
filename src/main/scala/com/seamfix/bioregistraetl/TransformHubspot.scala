package com.seamfix.bioregistraetl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TransformHubspot(sparkSession: SparkSession) extends TransformData {

  /**
    * Given a parquet file path, read the parquet into DataFrame.
    *
    * @param path
    * @return
    */
  def readParquet(path: String): DataFrame = sparkSession.read.parquet(path)

  private def explodeChangeColumn(columnName: String = "changes", dataFrame: DataFrame) = {
    val columnList = List("changes", "causedByEvent")
    dataFrame
      .alias("changeColumns")
      .withColumn("exploded", explode(col(columnName)))
      .select("changeColumns.*", "exploded.*")
      .alias("firstExploded")
      .select("firstExploded.*",
              "firstExploded.causedByEvent.created",
              "firstExploded.causedByEvent.id")
      .withColumnRenamed("created", "event_created_time")
      .withColumnRenamed("id", "event_id")
      .drop(columnList: _*)
  }

  def explodeCampaigns(dataFrame: DataFrame): DataFrame = {
    dataFrame.select(flattenSchema(dataFrame.schema):_*)
  }

  def explodeChanges(dataFrame: DataFrame): DataFrame =
    transform(dataFrame, explodeChangeColumn, "changes")
}

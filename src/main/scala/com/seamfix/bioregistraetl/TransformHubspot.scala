package com.seamfix.bioregistraetl

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TransformHubspot(sparkSession: SparkSession) extends TransformData {

  /**
    * Given a parquet file path, read the parquet into DataFrame.
    *
    * @param path
    * @return
    */
  def readParquet(path: String): DataFrame = sparkSession.read.parquet(path)

  private val renameCol = (c: Column) => c.alias(c.toString.replace(".", "__"))

  private val extractOtherArray: DataFrame => DataFrame = (c: DataFrame) => {
    var dataFrame = c
    extractArrayCol(dataFrame.schema).foreach(column =>
      dataFrame = dataFrame.withColumn(column + "_value", explode(col(column))).drop(column)
    )
    dataFrame
  }

  def explodeCampaigns(dataFrame: DataFrame): DataFrame = {
    extractOtherArray(dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*))
  }

  def explodeChanges(dataFrame: DataFrame): DataFrame =
    extractOtherArray(dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*))

  def explodeCompanies(dataFrame: DataFrame): DataFrame =
    extractOtherArray(dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*))

  def explodeContacts(dataFrame: DataFrame): DataFrame = {
    val schemas = flattenSchema(dataFrame.schema).toSet.diff(
      Set("identity-profiles.identities.timestamp",
          "identity-profiles.identities.type",
          "identity-profiles.identities.value").map(name => col(name)))
    dataFrame.select(schemas.toSeq.map(renameCol): _*)
  }

  def explodeAll(dataFrame: DataFrame): DataFrame =
   dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*)

  def explodeContactsList(dataFrame: DataFrame): DataFrame =
   dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*).drop("filters")

  def explodeDealPipelines(dataFrame: DataFrame): DataFrame =
    extractOtherArray(dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*))

  def explodeDeals(dataFrame: DataFrame): DataFrame =
    extractOtherArray(dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*))

  def explodeEmailEvents(dataFrame: DataFrame): DataFrame =
    dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*)

  def explodeEngagements(dataFrame: DataFrame) =
      dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*)


  def explodeOwners(dataFrame: DataFrame) =
    extractOtherArray(dataFrame.select(flattenSchema(dataFrame.schema).map(renameCol): _*))



}

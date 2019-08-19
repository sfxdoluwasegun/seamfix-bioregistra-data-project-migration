package com.seamfix.bioregistraetl

import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait ReadFormat
case object ParquetFormat extends ReadFormat
case object JsonFormat    extends ReadFormat

object RetrieveData extends App {

  val ACCESSKEYID = sys.env("AWS_ACCESS_KEY_ID")
  val ACCESSKEYSECRET = sys.env("AWS_SECRET_ACCESS_KEY")

  val spark = SparkSession.builder
    .appName("retrieve-data")
    .master("local[*]")
    .getOrCreate()

  // Makes $ StringContext coming from import spark.implicits._ available use col(StringName) otherwise
  val sc = spark.sparkContext
  sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
  sc.hadoopConfiguration.set("parquet.enable.dictionary", "false")

//  /**
//  * Using RetrieveData Helper Object you can readWrite Json file to s3
//    */
//  val path = "s3a://seamfix-machine-learning-ir/bioregistra_hubspot/subscription_changes/*.jsonl"
//  val parquetPath = "s3a://seamfix-machine-learning-ir/bioregistra_hubspot/contact_lists/*.jsonl"
//  val explodedDf = RetrieveDataHelper.readWriteJson(parquetPath, spark, true, true, JsonFormat)
//  explodedDf.printSchema()
//  explodedDf.show(10)



  //Working on the DataFrame:
  val firstCleaningJob = new FirstCleaningJob(spark)
  firstCleaningJob.callGetDistinctCategories.show(10)
  val categoryCount = firstCleaningJob.categories
  //Write
  writeVisualizationParquet(categoryCount, "projects_group", spark)



  def writeVisualizationParquet(dataFrame: DataFrame, s3Folder:String, spark:SparkSession) = {
    val path = s"s3a://seamfix-machine-learning-ir/BioregistraParquet/visualization_parquet/$s3Folder"
    dataFrame.write.mode("append").parquet(path)
  }




}

package com.seamfix.bioregistraetl

import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait ReadFormat
case object ParquetFormat extends ReadFormat
case object JsonFormat    extends ReadFormat

object RetrieveData extends App {

  val ACCESSKEYID     = sys.env("AWS_ACCESS_KEY_ID")
  val ACCESSKEYSECRET = sys.env("AWS_SECRET_ACCESS_KEY")

  val spark = SparkSession.builder
    .appName("retrieve-data")
    .master("local[*]")
//    .config("spark.sql.parquet.compression.codec", "gzip")
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

//  val sign_up = spark.read
//    .format("jdbc")
//    .option(
//      "url",
//      "jdbc:postgresql://bioregistra-dbserver.postgres.database.azure.com:5432/br?user=seamfix@bioregistra-dbserver&password=*s3amf1x#2019")
//    .option("query", "SELECT su.* FROM br.sign_up su")
//    .load()
//  val signUpRenamed = sign_up.withColumnRenamed("number of forms", "num_forms")
//
//  signUpRenamed.write
//    .option("compression", "gzip")
//    .mode("append")
//    .parquet(
//      "s3a://seamfix-machine-learning-ir/BioregistraParquet/bioregistra_db_parquet/sign_up_" + "parquet")
//
//  val forms = spark.read
//    .format("jdbc")
//    .option(
//      "url",
//      "jdbc:postgresql://bioregistra-dbserver.postgres.database.azure.com:5432/br?user=seamfix@bioregistra-dbserver&password=*s3amf1x#2019")
//    .option("query", "SELECT fo.* FROM br.forms fo")
//    .load()
//
//  forms.write
//    .option("compression", "gzip")
//    .mode("append")
//    .parquet(
//      "s3a://seamfix-machine-learning-ir/BioregistraParquet/bioregistra_db_parquet/forms" + "parquet")


  //Updated Sign ups
  val updatedSignUps = spark.read
    .format("jdbc")
    .option(
      "url",
      "jdbc:postgresql://bioregistra-dbserver.postgres.database.azure.com:5432/br?user=seamfix@bioregistra-dbserver&password=*s3amf1x#2019")
    .option("query", "SELECT su.* FROM br.sign_ups su")
    .load()
  val updatedSignUpRenamed = updatedSignUps.withColumnRenamed("number of forms", "num_forms")
      .withColumnRenamed("signed up from template?", "sign_up_template")

  updatedSignUpRenamed.write
    .option("compression", "gzip")
    .mode("overwrite")
    .parquet(
      "s3a://seamfix-machine-learning-ir/BioregistraParquet/bioregistra_db_parquet/sign_up_" + "parquet")


  val updatedForms =  spark.read
    .format("jdbc")
    .option(
      "url",
      "jdbc:postgresql://bioregistra-dbserver.postgres.database.azure.com:5432/br?user=seamfix@bioregistra-dbserver&password=*s3amf1x#2019")
    .option("query", "SELECT fo.* FROM br.form fo")
    .load()

  updatedForms.write
    .option("compression", "gzip")
    .mode("overwrite")
    .parquet(
      "s3a://seamfix-machine-learning-ir/BioregistraParquet/bioregistra_db_parquet/forms" + "parquet")


  updatedSignUpRenamed.printSchema()
  updatedForms.printSchema()
//  //Working on the DataFrame:
//  val firstCleaningJob = new FirstCleaningJob(spark)
////  firstCleaningJob.callGetDistinctCategories.show(10)
////  val categoryCount = firstCleaningJob.categories
////  //Write
////  //writeVisualizationParquet(categoryCount, "projects_group", spark)
////
////  //Payments Group
//  val paymentGroups = firstCleaningJob.payingOrganizations
//  writeVisualizationParquet(paymentGroups, "subscription_plans", spark)
//
//
//
//  def writeVisualizationParquet(dataFrame: DataFrame, s3Folder:String, spark:SparkSession) = {
//    val path = s"s3a://seamfix-machine-learning-ir/BioregistraParquet/visualization_parquet/$s3Folder"
//    dataFrame.write.mode("append").parquet(path)
//  }

}

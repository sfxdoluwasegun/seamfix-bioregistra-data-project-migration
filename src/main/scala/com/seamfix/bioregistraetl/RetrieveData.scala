package com.seamfix.bioregistraetl

import org.apache.spark.sql.{DataFrame, SparkSession}

object RetrieveData extends App {

  val ACCESSKEYID     = sys.env("AWS_ACCESS_KEY_ID")
  val ACCESSKEYSECRET = sys.env("AWS_SECRET_ACCESS_KEY")

  val spark = SparkSession.builder
    .appName("retrieve-data")
    .master("local[*]")
    .getOrCreate()

   // Makes $ StringContext coming from import spark.implicits._ available use col(StringName) otherwise
  val sc = spark.sparkContext
  sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

  val path = "s3a://seamfix-machine-learning-ir/bioregistra_hubspot/subscription_changes/*.jsonl"

  /**
    * Split a path and extract is folder name
    *
    * @param path
    * @return
    */
  def splitPath(path: String) = {
    val folderPath = path.substring(0, path.lastIndexOf("/"))
    val folderName = folderPath.split("/").lastOption match {
      case Some(value) => value
      case _ => {
        val splits = folderPath.split("/")
        splits(splits.length - 1)
      }
    }
    folderName
  }

  /**
    * Load Data from s3 and dump job to parquet.
    */
  def readWriteJson(path: String, spark: SparkSession, write: Boolean = false): DataFrame =
    if (write) {
      val folderName = splitPath(path)
      val folderPath = path.substring(0, path.lastIndexOf(folderName))
      val df         = spark.read.option("multiline", "true").json(path)
      df.write.mode("append").parquet(folderPath + folderName + "parquet")
      df
    } else spark.read.option("multiline", "true").json(path)

  val parquetPath = "s3a://seamfix-machine-learning-ir/BioregistraParquet/hubsport/owners/"

  val tranformer = new TransformHubspot(spark)
  val data = tranformer.readParquet(parquetPath)
  data.schema.printTreeString()

  val exploded = tranformer.explodeOwners(data)

  exploded.printSchema()

  exploded.show(10)
}
//  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", ACCESSKEYID)
//  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", ACCESSKEYSECRET)
//
//  val path = s"s3a://seamfix-machine-learning-ir/bioregistra-hubsport"
//  val fileSystem = FileSystem.get(URI.create(path), new Configuration())
//  val it = fileSystem.listFiles(new Path(path), true)
//  while(it.hasNext) {
//    val path = it.next().getPath.toUri.getPath
//    println(path)
//  }

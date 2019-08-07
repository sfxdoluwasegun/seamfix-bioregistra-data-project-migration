package com.seamfix.bioregistraetl

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

trait SparkContextSetup {

  def withSparkContext(testMethod: SparkContext => Any) {

    val spark = SparkSession
      .builder
      .appName("Spark-test")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    try {
      testMethod(sc)
    } finally sc.stop()
  }

  def withSparkSession(testMethod: SparkSession => Any): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark-test")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    try {
      testMethod(spark)
    } finally spark.close()

  }
}
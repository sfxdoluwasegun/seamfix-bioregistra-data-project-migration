package com.seamfix.bioregistraetl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.IntegerType

class BuildClassifier(spark: SparkSession) {

  val org_sub      = spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/orgSub/")
  val churn_table  = spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/churntable/")
  val org_status   = spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/orgStatus/")
  val project_date = spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/projectDate/")
  val project_per_org_view =
    spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/projectOrgView/")
  val project_view = spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/projectView/")
  val subscription_plan =
    spark.read.parquet("/mnt/irelangS3Data/BioregistraParquet/subscriptionPlan/")

  val getHoursPentToDate = (x: Column, y: Column) => {
    (unix_timestamp(y) - unix_timestamp(x)) / 3600
  }
  val daysSpent = (x: Column) => x / 24
  val org_df_labels = org_status
    .withColumn("hours_spent", getHoursPentToDate(col("sign_up_date"), col("last_date")))
    .withColumn("days_spent", daysSpent(col("hours_spent")).cast(IntegerType))
    .withColumn("label",
                when(col("days_spent") === 14, "trial")
                  .when(col("days_spent") > 14, "subscribed")
                  .otherwise("registerOnly"))
  org_df_labels.show()


  //Count distinct labels
  org_df_labels.select("label").distinct.count
  org_df_labels.where(col("label") === "registerOnly").count
  println(org_df_labels.groupBy(col("label")).count)


  println(org_df_labels.where(col("days_spent") > 14).count)
  println(org_df_labels.where(col("days_spent") === 14).count)

}

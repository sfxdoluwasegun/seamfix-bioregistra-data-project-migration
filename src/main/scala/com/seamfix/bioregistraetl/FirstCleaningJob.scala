package com.seamfix.bioregistraetl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

/**
  *
  * The idea is to look at the tables available and write a spark script to generate a clean table for visualization
  * Hence you will load multiple dataframes depending on the table you find useful
  * Clean the dataframe and make your final visualization.
  */
class FirstCleaningJob(spark: SparkSession) {
  //TODO: This should go into the configuration file
  private val PROJECT_PATH =
    "s3a://seamfix-machine-learning-ir/BioregistraParquet/bioregistra_db_parquet/projects/"
  private val SUBSCRIPTION_HISTORY =
    "s3a://seamfix-machine-learning-ir/BioregistraParquet/bioregistra_db_parquet/subscription_payment_history/"

  //Read a DataFrame and give it the name of the path
  def readDf(path: String) = spark.read.parquet(path)
  private lazy val expression: DataFrame => Map[String, String] = (df: DataFrame) =>
    df.columns.map(_ -> "approx_count_distinct").toMap

  def selectColumns(df: DataFrame, columns: Seq[String]): DataFrame = {
    val toSelectColumns = columns.map(x => col(x))
   df.select(toSelectColumns: _*)
  }

  //Read Project table
  lazy val projectDf      = readDf(PROJECT_PATH)
  lazy val paymentHistory = readDf(SUBSCRIPTION_HISTORY)

  //1.: Get Count of of total category
  lazy val dfToCheckDistinct         = selectColumns(projectDf, Seq("category"))
  lazy val callGetDistinctCategories = dfToCheckDistinct.agg(expression(dfToCheckDistinct))

  /**
    * //Shows we have 12 Categories of industries in the project so far
    *|approx_count_distinct(category)|
    *+-------------------------------+
    *|                             12|
    *+-------------------------------+
    */
  //Get count per categories
  lazy val categories = dfToCheckDistinct.groupBy("category")

  /**
    * +--------------------+-----+
    * |            category|count|
    * +--------------------+-----+
    * |              OTHERS|  405|
    * |          GOVERNMENT|  109|
    * |                null|  140|
    * |         HOSPITALITY|   48|
    * |                FMCG|   50|
    * |            PERSONAL|  108|
    * |           EDUCATION|  157|
    * |   TELECOMMUNICATION|  132|
    * |FINANCIAL INSTITU...|  247|
    * |            RESEARCH|  123|
    * |         REAL ESTATE|   43|
    * |            SECURITY|   91|
    * |          CORPORATES|  144|
    * +--------------------+-----+
    *
    * Companies can host multiple project and even in different categories
    */
  //Get company company that subscribed multiple time and their project sector
  //Project has a start date, orgId

  lazy val payingOrganizations = paymentHistory
    .groupBy(col("orgId"), col("subscriptionplanid"))
    .agg(count(col("subscriptionplanid")).alias("countSubscriptionPlan"))


}

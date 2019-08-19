package com.seamfix.bioregistraetl

import org.apache.spark.sql.{DataFrame, SparkSession}

object RetrieveDataHelper {

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
    * Load Data from s3 and dump job to parquet. works for all hobspot data except ContactList
    */
  def readWriteJson(path: String,
                    spark: SparkSession,
                    write: Boolean = false,
                    is_transformed: Boolean = false,
                    read_file_format: ReadFormat): DataFrame =
    if (write) {
      val transformer = new TransformHubspot(spark)
      val folderName = splitPath(path)
      val folderPath = path.substring(0, path.lastIndexOf(folderName))

      val readDf = read_file_format match {
        case JsonFormat => spark.read.json(path)
        case _ => transformer.readParquet(path)
      }
      if (is_transformed) {
        val df = transformer.explodeContactsList(readDf)
        df.write.mode("append").parquet(folderPath + folderName + "processed_parquet")
        df
      } else {
        readDf.write.mode("append").parquet(folderPath + folderName + "parquet")
        readDf
      }

    } else spark.read.option("multiline", "true").json(path)

}
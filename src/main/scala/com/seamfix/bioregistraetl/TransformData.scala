package com.seamfix.bioregistraetl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col

trait TransformData {


  def readParquet(path: String): DataFrame


  /**
    * Transform a column of a dataframe with a function that takes a String and returns a DataFrame
    * @return
    */
  def transform(dataFrame: DataFrame, processDF: (String, DataFrame) => DataFrame, column: String): DataFrame = {
    if (dataFrame.columns.contains(column)) processDF(column, dataFrame)
    else {
      dataFrame
    }
  }

  def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val colName = if (prefix == null) f.name else (prefix + "." + f.name)

      f.dataType match {
        case st: StructType => flattenSchema(st, colName)
        case _ => Array(col(colName))
      }
    })
  }

}
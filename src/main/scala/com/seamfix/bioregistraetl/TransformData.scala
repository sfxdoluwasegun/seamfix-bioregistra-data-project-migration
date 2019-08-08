package com.seamfix.bioregistraetl

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.functions._

trait TransformData {


  /**
  * Extract only the first layer ArrayType in a DataFrame
    */
  def extractArrayCol(schema: StructType) = schema.fields.flatMap{
    case StructField(name, ArrayType(_, _), _, _) => Seq(s"$name")
    case _ => Seq.empty[String]
  }


  def readParquet(path: String): DataFrame
  /**
    * Transform a column of a dataframe with a function that takes a String and returns a DataFrame
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

      f match {
        case StructField(_, struct:StructType, _, _) => flattenSchema(struct, colName)
        case StructField(_, ArrayType(x :StructType, _), _, _) => flattenSchema(x, colName)
        case StructField(_, ArrayType(x, _), _, _) => Array(col(colName))
        case _ => Array(col(colName))
      }
    })
  }

}
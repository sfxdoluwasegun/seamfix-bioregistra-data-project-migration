package com.seamfix.bioregistraetl

/**
 * A simple test for everyone's favourite wordcount example.
 */
import org.scalatest.FunSuite

class WordCountTest extends FunSuite with SparkContextSetup {

  test("should read parquet from s3 with sparkSession"){ withSparkSession{ session =>
    val start = System.currentTimeMillis()
    val path = "s3a://seamfix-machine-learning-ir/BioregistraParquet/hubsport/subscription_changes/"
    val timeToReadParquet = System.currentTimeMillis() - start
    val startReadJson = System.currentTimeMillis()
    val data = session.read.option("multiline", "true").json("s3a://seamfix-machine-learning-ir/bioregistra_hubspot/subscription_changes/*.jsonl")
    val timeToReadJson = System.currentTimeMillis() - startReadJson
    val transformer = new TransformHubspot(session)
    val dataFrame = transformer.readParquet(path)
    assert(dataFrame.columns.length > 0)
    assert(dataFrame.columns.contains(data.columns.head))
    assert(timeToReadJson > timeToReadParquet)
  }}
}


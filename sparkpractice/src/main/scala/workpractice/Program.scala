package com.practice.sparkpractice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import scala.io.Source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import workpractice.LoudCloudTest
import workpractice.SparkFunctions
/*import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec
import org.scalatest.FunSuite
import org.scalatest.Assertions._
import com.holdenkarau.spark.testing.RDDComparisons*/

object CountingLocalApp
{
  
	def main(args : Array[String])
	{
    //var lcSparkTest : LoudCloudTest = new LoudCloudTest
    //lcSparkTest.SolveProblem()
    
    var sparkFunc : SparkFunctions = new SparkFunctions
    sparkFunc.runTransformationActionFunctions()
    //sparkFunc.runDataFrameFunctions()
    
	}
}

/*class SampleRDDTest extends FunSuite with SharedSparkContext with RDDComparisons
{
  test("really simple transformation") {
    val input = List("hi", "hi holden", "bye")
    val expected = List(List("hi"), List("hi", "holden"), List("bye"))

    assert(tokenize(sc.parallelize(input)).collect().toList === expected)
  }
  def tokenize(f: RDD[String]) = {
    f.map(_.split(" ").toList)
  }
}*/



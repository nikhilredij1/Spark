package workpractice

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

class LoudCloudTest
{
  var df_item : org.apache.spark.sql.DataFrame = null
  var df_data : org.apache.spark.sql.DataFrame= null
  var df_user : org.apache.spark.sql.DataFrame= null
  var df_genre : org.apache.spark.sql.DataFrame= null
  
  def SolveProblem()
  {
    val sparkConf = new SparkConf().setAppName("SparkJDBCDemo").setMaster("local")
    if(sparkConf != null) 
    {
      val sparkCxt = new SparkContext(sparkConf)
      if(sparkCxt != null) 
      {
        val sqlCxt = new SQLContext(sparkCxt)
        if(sqlCxt != null) 
        {
          val df_item_temp = sqlCxt.read.format("csv").option("delimiter", "|").load("input\\u.item")
          df_item = df_item_temp.where(col("_c2").isNotNull)
          df_data = sqlCxt.read.format("csv").option("delimiter", "\t").load("input\\u.data")
          df_user = sqlCxt.read.format("csv").option("delimiter", "|").load("input\\u.user")
          df_genre = sqlCxt.read.format("csv").option("delimiter", "|").load("input\\u.genre")
          
	        Problem1()
		      //Problem2()
		      //Problem3()
		      //Problem4()
		      //Problem5()
		      //Problem6(sparkCxt)
          //Problem7()
          //Problem8()
          //Problem9()
          GetAgeGroup("49")
        }
      }
    }
  }
  
  def Problem1()
	{
		try
		{
			df_item.drop("_c4","_c3").show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
	}
	
	def Problem2()
	{
		try
		{
			val df_movie_date = df_item.select(col("_c1").as("MovieName"), to_date(col("_c2"), "dd-MMM-yyyy").cast("date").as("Date"))
			val df_year_month_movie = df_movie_date.where(col("Date").isNotNull).select(year(col("Date")).as("Year"), month(col("Date")).as("Month"), col("MovieName")) 
			df_year_month_movie.orderBy("Year", "Month").show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
	}
	
	def Problem3()
	{
		try
		{
			val df_movie_date = df_item.select(col("_c1").as("MovieName"), to_date(col("_c2"), "dd-MMM-yyyy").cast("date").as("Date"))
			val df_year_movie = df_movie_date.where(col("Date").isNotNull).select(year(col("Date")).as("Year"), col("MovieName")) 
			df_year_movie.orderBy("Year").show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
  }
	
	def Problem4()
	{
		try
		{
		  val df_movie_date = df_item.select(col("_c0").as("MId"),col("_c1").as("MovieName"), to_date(col("_c2"), "dd-MMM-yyyy").cast("date").as("Date"))
			val df_year_movie = df_movie_date.where(col("Date").isNotNull).select(col("MId"), col("MovieName"), year(col("Date")).as("Year"))

			val df_movieid_rating = df_data.where(col("_c2") === "4" || col("_c2") === "5")
			.withColumn("rating4", when(col("_c2") === "4", 1).otherwise(0))
			.withColumn("rating5", when(col("_c2") === "5", 1).otherwise(0))
			.select(col("_c1").as("MId"), col("rating4"), col("rating5"))
			
			val df_movieid_rating_groupby_id = df_movieid_rating.groupBy(col("MId"))
			.agg(sum("rating4").as("4 rating"), sum("rating5").as("5 rating"))
			.where(col("4 rating").cast(DataTypes.IntegerType).+(col("5 rating").cast(DataTypes.IntegerType)).>(5))
			
			val df_join = df_year_movie.join(df_movieid_rating_groupby_id, Seq("MId")).drop(col("MId"))
			df_join.orderBy(col("MovieName")). show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
  }
	
	def Problem5()
	{
		try
		{
		  val df_date_movie = df_item.select(to_date(col("_c2"), "dd-MMM-yyyy").cast("date").as("Date"), col("_c1").as("MovieName"))
		  val df_month_year_movie = df_date_movie.where(col("Date").isNotNull)
		  .select(month(col("Date")).as("Month"), year(col("Date")).as("Year"), col("MovieName"))
		  
		  val df_month_year_movie_updated = df_month_year_movie.withColumn("Vowels Not e", 
		      when(col("MovieName").like("%a%"),1).otherwise(0) 
		      + when(col("MovieName").like("%i%"),1).otherwise(0) 
		      + when(col("MovieName").like("%o%"),1).otherwise(0) 
		      + when(col("MovieName").like("%u%"),1).otherwise(0))
		  df_month_year_movie_updated.where(col("Vowels Not e").>(1)) .orderBy("Year", "Month").show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
  }
	
	def Problem6(sc : SparkContext)
	{
	  try
		{
	    val spark = SparkSession.builder().master("local").getOrCreate()
      import spark.implicits._
      val schema1 = StructType(
      StructField("MovieName", StringType, true) ::
      StructField("Genre", StringType, true) :: Nil)
      var df_movie_genre = spark.createDataFrame(sc.emptyRDD[Row], schema1)
      for(i <- 0 to 18)
      {
        val genre = df_genre.take(i+1).last.get(0)
        val df_temp = df_item.where(col("_c"+(i+5)).===(1)).select(col("_c1").as("MovieName"), when(col("_c"+(i+5)).===(1), genre).as("Genre"))
        df_movie_genre = df_movie_genre.union(df_temp)
      }
      df_movie_genre.orderBy("Genre").show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
	}
	
	def Problem7()
	{
	  try
		{
	    val df_movie_date = df_item.select(col("_c0").as("MId"), col("_c1").as("MovieName"), to_date(col("_c2"), "dd-MMM-yyyy").cast("date").as("Date"))
	    val df_movie_year = df_movie_date.select(col("MId"), col("MovieName"), year(col("Date")).as("Year"))
	    
	    val df_movie_rating = df_data.where(col("_c2").===("4") || col("_c2").===("5"))
	    .withColumn("Rating4", when(col("_c2").===("4"),1).otherwise(0))
	    .withColumn("Rating5", when(col("_c2").===("5"),1).otherwise(0))
	    .withColumn("Rating45", when(col("_c2").===("5") || col("_c2").===("4"),1).otherwise(0))
	    .select(col("_c1").as("MId"), col("Rating4"), col("Rating5"), col("Rating45"))
	    .groupBy("MId")
	    .agg(sum(col("Rating4")).as("4 Rating"), sum(col("Rating5")).as("5 Rating"), sum(col("Rating45")).as("45 Rating"))
	    
	    val df_movie_joined = df_movie_year.join(df_movie_rating, Seq("MId")).drop(col("MId"))
	    
	    val df_partition = Window.partitionBy(col("Year")).orderBy(col("Year"), col("45 Rating").desc)
	    
	    val df_final = df_movie_joined.withColumn("rownum", row_number().over(df_partition))
	    .drop(col("45 Rating"))
	    .where(col("rownum").<=(3))
	    .drop(col("rownum"))
	    
	    df_final.show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
	}
	
	def Problem8()
	{
	  try
		{
	    val df_user_occup = df_user.select(col("_c0").as("UId"), col("_c3").as("Occupation"))
	    val df_user_data = df_data.select(col("_c0").as("UId"))
	    
	    df_user_data.join(df_user_occup, Seq("UId"))
	    .groupBy("Occupation")
	    .agg(count(col("UId")).as("Count")).show()
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
	}
	
	def Problem9()
	{
	  try
		{
	    val df_mov_rating = df_data.select(col("_c0").as("UId"), col("_c1").as("MId"), col("_c2").as("Rating"))
	    val df_user_mov_rating = df_mov_rating.where(col("Rating").===(4) || col("Rating").===(5))
	    .withColumn("Rating4", when(col("Rating").===(4),1).otherwise(0))
	    .withColumn("Rating5", when(col("Rating").===(5),1).otherwise(0))
	    .withColumn("Rating45", when(col("Rating").===(4) || col("Rating").===(5),1).otherwise(0))
	    .drop(col("Rating"))
	    
	    val df_user_age = df_user.select(col("_c0").as("UId"), col("_c1").as("Age"))
	    val df_user_agegrp = df_user_age
	    .where(col("Age").>=(1) && col("Age").<=(80))
	    .withColumn("AgeGroup", when(col("Age"), GetAgeGroup(col("Age").toString())))
		}
		catch
		{
		  case ex : Throwable => ex.printStackTrace()
		}
	}
	
	def GetAgeGroup(age : String) : String =
	{
	  var rem = age.toInt%5
	  var num = age.toInt/5
	  println(rem,num)
	  null
	}  
}
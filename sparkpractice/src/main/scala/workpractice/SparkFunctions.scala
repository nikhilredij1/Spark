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
import org.apache.spark.sql.catalyst.expressions.Coalesce


class SparkFunctions 
{
    val sparkConf = new SparkConf().setAppName("SparkDemo1").setMaster("local")
    val sparkCxt = new SparkContext(sparkConf)
    val sqlCxt = new SQLContext(sparkCxt)
    
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    
    def runTransformationActionFunctions()
    {
      val rdd1 = sparkCxt.parallelize(Array("b", "a", "c"))
      val rddMap = rdd1.map(z => (z,1))
      //println("Before Map " + rdd1.collect())
      //println("After Map " + rddMap.collect())
      
      val rdd2 = spark.read.textFile("input/spark_test.txt").rdd
      val rddFlatmap = rdd2.flatMap(lines => lines.split(" "))
      //rddFlatmap.foreach(println)
      
      val rdd3 = sparkCxt.parallelize(Array(1,2,3))
      val rddFilter = rdd3.filter(n => n%2 == 1)
      //println("Before Filter " + rdd3.collect().mkString("| "))
      //println("After Filter " + rddFilter.collect().mkString("| "))
      
      val rdd4 = sparkCxt.parallelize(Seq((1,"jan",2016),(3,"nov",2014),(16,"feb",2014)))
      val rdd5 = sparkCxt.parallelize(Seq((5,"dec",2014),(17,"sep",2015)))
      val rddUnion = rdd4.union(rdd5)
      //rddUnion.foreach(println)
      
      val rdd6 = sparkCxt.parallelize(Array("John", "Fred", "Anna", "James"))
      val rddGroupBy = rdd6.groupBy(w => w.charAt(0))
      println("Before GroupBy " + rdd6.collect().mkString("| "))
      println("After GroupBy " + rddGroupBy.collect().mkString("| "))
    }
    
    def runDataFrameFunctions()
    {
      val dataList = Seq("Joe|196|242|3|22012016|-7.8|Texas|",
          "Samuel|186|302|3|02032016|1.67|California|",
          "Linda|22|377|1|15082016|17.7||",
          "Bruce|244|51|2|07072016|-9.60||",
          "Clark|166|346|1|14042016|3.34||",
          "Deadpool|298|474|4|26092016|4.44|New York|",
          "Tony|115|265|2|05012016|67.9|Texas|",
          "Peter|253|465|5|11032016|-5.13|Chicago|",
          "John|305|451|3|29102016|91.6|Seattle|",
          "Arthur|6|86|1|06112016|45.7||")
      .toDF("col")
      
      val iniDF = dataList
      .withColumn("user_name", split(col("col"), "\\|").getItem(0))
      .withColumn("user_id", split(col("col"), "\\|").getItem(1).cast(IntegerType))
      .withColumn("mov_id", split(col("col"), "\\|").getItem(2).cast(IntegerType))
      .withColumn("review", split(col("col"), "\\|").getItem(3).cast(IntegerType))
      .withColumn("strdate", split(col("col"), "\\|").getItem(4))
      .withColumn("decimal", split(col("col"), "\\|").getItem(5).cast(DoubleType))
      .withColumn("location_old", split(col("col"), "\\|").getItem(6))
      .drop(col("col"))
      
      val mainDF = iniDF.withColumn("date", to_date(col("strdate"),"ddMMyyyy")) //to_date function to convert from string to date
      .withColumn("location", when(col("location_old").equalTo(""), null).otherwise(col("location_old")))
      .drop(col("strdate"))
      .drop(col("location_old"))
      
      val usernameDF = mainDF.drop(col("user_id")).drop(col("mov_id")).drop(col("review")).drop(col("date")).drop(col("decimal")).drop(col("location"))
      val useridDF = mainDF.drop(col("user_name")).drop(col("mov_id")).drop(col("review")).drop(col("date")).drop(col("decimal")).drop(col("location"))
      val movidDF = mainDF.drop(col("user_name")).drop(col("user_id")).drop(col("review")).drop(col("date")).drop(col("decimal")).drop(col("location"))
      val reviewDF = mainDF.drop(col("user_name")).drop(col("user_id")).drop(col("mov_id")).drop(col("date")).drop(col("decimal")).drop(col("location"))
      val dateDF = mainDF.drop(col("user_name")).drop(col("user_id")).drop(col("mov_id")).drop(col("review")).drop(col("decimal")).drop(col("location"))
      val decimalDF = mainDF.drop(col("user_name")).drop(col("user_id")).drop(col("mov_id")).drop(col("review")).drop(col("date")).drop(col("location"))
      val locationDF = mainDF.drop(col("user_name")).drop(col("user_id")).drop(col("mov_id")).drop(col("review")).drop(col("date")).drop(col("decimal"))
      val useridlocationDF = mainDF.drop(col("user_name")).drop(col("mov_id")).drop(col("review")).drop(col("date")).drop(col("decimal"))
      val useridreviewDF = mainDF.drop(col("user_name")).drop(col("mov_id")).drop(col("date")).drop(col("decimal")).drop(col("location"))
      
      
      val absDF = decimalDF
      .withColumn("abs_value_of_decimal", abs(col("decimal")))  //abs function takes column of type number
      //absDF.show()
      
      /*-------------Date Functions---------------*/
      
      val addedMonths = 5
      val addedDays = 10
      val subtractedDays = 15
      val date_addDF = dateDF
      .withColumn("add_" + addedDays + "_Days", date_add(col("date"), addedDays))  // Adds n days to date
      .withColumn("sub_" + subtractedDays + "_Days", date_sub(col("date"), subtractedDays))// Subtracts n days from date
      .withColumn("added_"+addedMonths+"_Months", add_months(col("date"), addedMonths))// Adds n days to date
      .withColumn("current_timestamp", current_timestamp())  // current_timestamp returns current timestamp  as Timestamp column
      .withColumn("current_date", current_date())  // current_date returns current date  as Date column
      .withColumn("diff(current_date,date)", datediff(current_date(), col("date")))  // Date difference between 2 dates. Date1 - Date2
      .withColumn("date_format_dd.MM.yyyy", date_format(col("date"), "dd.MM.yyyy"))  // convert date into any other format. Always put month in 'MM'/'MMM' not in 'mm'
      .withColumn("year", year(col("date")))  // get year from date
      .withColumn("month", month(col("date")))  // get month from date
      .withColumn("quarter", quarter(col("date")))  // get quarter from date
      .withColumn("dayofmonth", dayofmonth(col("date")))  // get dayofmonth from date
      .withColumn("dayofyear", dayofyear(col("date")))  // get dayofyear from date
      //date_addDF.show()
      
      val arrayDF = mainDF.withColumn("arr", array(col("user_id"), col("mov_id")))  // new column as an array
      //arrayDF.show()

      // Filter DF according to value in column
      val filterDF = mainDF.where(col("review").===(1))
      val filterDF1 = mainDF.where(col("review").contains(1))
      val filterDF2 = mainDF.where(col("review").equalTo(1))
      val filterDF3 = mainDF.filter(col("review").equalTo(1))
      //filterDF.show()
      
      //val array_containsDF = reviewDF.withColumn("rev", array_contains($"review",1)) -- array_contains Issue 
      
      val ascDF = mainDF.sort(asc("user_id"))  // Sort column ascendingly
      //ascDF.show()
      
      val descDF = mainDF.sort(desc("user_id"))  // Sort column descendingly
      //descDF.show()
      
      val asciiDF = usernameDF.withColumn("asciiname", ascii(col("user_name")))  //Computes the numeric value of the first character of the string column
      //asciiDF.show()
      
      val avgDF = useridDF.withColumn("avg", ascii(col("user_id")))
      //avgDF.show() 
      
      
      // Coalesce is used to return a new column. It takes number of columns as parameters
      // It checks if first parameter is cell value is null or not, if null it returns the next parameter cell value
      // When using Coalesce for more than one DataFrames we have to join those dataframes in order to pass another value for same cell

      
      var coalesceDF1 = useridlocationDF.join(mainDF, useridlocationDF("user_id").equalTo(mainDF("user_id")))
      .select(coalesce(useridlocationDF("location"), mainDF("user_name")).alias("coalesceCol"))
      var coalesceDF2 = mainDF.select(coalesce(mainDF("location"), mainDF("user_name")).alias("coalesceCol"))
      var coalesceDF3 = mainDF.select(coalesce(col("location"), col("user_name")).alias("coalesceCol"))
      var coalesceDF4 = mainDF.select(coalesce($"location", $"user_name").alias("coalesceCol"))
      var coalesceDF5 = locationDF.select(coalesce(col("location"), lit("user_name")).alias("coalesceCol"))
      var coalesceDF6 = locationDF.withColumn("coal", coalesce(col("location"), lit("user_name")).alias("coalesceCol")) 
      //coalesceDF1.show()
      
      // When Otherwise is used to check condition like 'If-Else' for columns
      // If we do not mention Otherwise condition then we will get null value for those rows
      var whenDF1 = mainDF.select(when(mainDF("location").isNotNull, mainDF("location")).otherwise(6).as("whenCol"))
      var whenDF2 = mainDF.select(when(mainDF("location").equalTo("Texas"), mainDF("location")).otherwise("Not Texas").as("whenCol"))
      var whenDF3 = mainDF.select(when(mainDF("location").equalTo("Texas"), "Address is Texas").as("whenCol"))
      //whenDF3.show()
      
      // WithColumn function adds a column in current DataFrame
      val litDF = locationDF.withColumn("litNumCol", lit(78))
      .withColumn("litStringCol", lit("blah"))
      //.withColumn("litStringCol", null) does not work
      //litDF.show()
      
      
      /*-------------Aggregate Functions---------------*/
      
      // collect_list is used for returning list of objects for value used in group by clause. List can have duplicate objects 
      val collect_listDF = mainDF.groupBy("location")
      .agg(collect_list(col("user_id")) as "id", collect_list(col("review")) as "list_review")
      //collect_listDF.show()
      
      val firstDF = mainDF.groupBy("location")
      .agg(collect_list(col("user_id")) as "id", first($"user_id") as "first_user_id")
      //firstDF.show()
      
      // collect_set is used for returning set of objects for value used in group by clause. Set will not have duplicate objects
      val collect_setDF = mainDF.groupBy("location")
      .agg(collect_set(col("user_id")) as "id", collect_set(col("review")) as "set_review")
      //collect_setDF.show()
      
      // concat_ws and concat are used to concatenate multiple columns
      // concat_ws needs parameter which passes separator text in concatenated text between column values
      val concat_wsDF = mainDF
      .withColumn("concat_wsCol", concat_ws("--", col("user_id"), col("review")))
      .withColumn("concatCol", concat(col("user_id"), lit("--"), col("review")))
      //concat_wsDF.show()
      
      // count function is used to get total count of values in columns with or without any condition
      val countDF = mainDF.select(count($"user_id") as "id_count", count(when($"review".>(1), true)) as "review_greaterthanone_count")
      //countDF.show()
      
      // countDistinct function is used to get count of distinct values in single column or group of columns
      val countDistinctDF1 = mainDF.select(countDistinct($"review") as "distinct_review_count")
      val countDistinctDF2 = mainDF.select(countDistinct($"review", $"location") as "distinct_review_location_count")
      //countDistinctDF1.show()
      
      val sortDF1 = useridreviewDF.sort($"user_id", $"review") // Sorts both ascendingly
      val sortDF2 = useridreviewDF.sort(asc("review"), desc("user_id")) 
      //sortDF2.show()
      
      val expDF = reviewDF.withColumn("e(2.71) ^ review", exp($"review"))
      //expDF.show()
      
      val factorialDF = reviewDF.withColumn("factorial", factorial($"review"))
      //factorialDF.show()
      
      val floorDF = decimalDF.withColumn("floor", floor($"decimal"))
      floorDF.show()
    }
    
}
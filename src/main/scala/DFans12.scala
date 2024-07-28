
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object DFans12 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val reviews = List(
      (1, 1),
      (2, 4),
      (3, 5)
    ).toDF("review_id", "rating")

    reviews.select(
      col("review_id"),
      col("rating"),
      when(col("rating")<3,"Bad")
        .when(col("rating")>=3 || col("rating")<=4,"Good")
        .otherwise("Excellent").alias("feedback"),
        when(col("rating")<3,"False")
        .when(col("rating")>=3 || col("rating")<=4,"True")
        .otherwise("Excellent").alias("is_positive")

    ).show()
  }
  }

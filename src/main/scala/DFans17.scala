import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, datediff, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans17 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val scores = List(
      (1, 85, 92),
      (2, 58, 76),
      (3, 72, 64)
    ).toDF("student_id", "math_score", "english_score")

    scores.select(
      col("student_id"),
      col("math_score"),
      col("english_score"),
      when(col("math_score")>80,"A")
        .when(col("math_score").between(60,80),"B")
        .otherwise("C").alias("math_grade"),
         when(col("english_score")>80,"A")
        .when(col("english_score").between(60,80),"B")
        .otherwise("C").alias("english_grade")

    ).show()
  }

}

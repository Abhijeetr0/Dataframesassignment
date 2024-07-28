import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans3 {
  def main(args: Array[String]): Unit ={
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val grades = List(
      (1, 85),
      (2, 42),
      (3, 73)
    ).toDF("student_id", "score")

    grades.select(
      col("student_id"),
      col("score"),
      when(col("score")>=50,"Pass")
        .otherwise("Fail").alias("Grade")
    ).show()
  }

}

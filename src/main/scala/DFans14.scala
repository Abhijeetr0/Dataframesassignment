import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, datediff, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans14 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val tasks = List(
      (1, "2024-07-01", "2024-07-10"),
      (2, "2024-08-01", "2024-08-15"),
      (3, "2024-09-01", "2024-09-05")
    ).toDF("task_id", "start_date", "end_date")

    tasks.select(
      col("task_id"),
      col("start_date"),
      col("end_date"),
      when(datediff(col("end_date"),col("start_date"))<7,"Short")
        .when(datediff(col("end_date"),col("start_date"))>=7 && datediff(col("end_date"),col("start_date"))<=14,"Medium")
        .otherwise("Long").alias("task_duration")
    ).show()
  }
}

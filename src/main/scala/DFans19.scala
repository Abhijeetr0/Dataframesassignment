import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, datediff, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans19 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val payments = List(
      (1, "2024-07-15"),
      (2, "2024-12-25"),
      (3, "2024-11-01")
    ).toDF("payment_id", "payment_date")
    payments.select(
      col("payment_id"),
      col("payment_date"),
      when(month($"payment_date").between(1,3), "Q1")
        .when(month($"payment_date").isin(4, 5, 6), "Q2")
        .when(month($"payment_date").isin(7,8,9), "Q3")
        .otherwise("Q4").alias("quarter")
    ).show()
  }


  }

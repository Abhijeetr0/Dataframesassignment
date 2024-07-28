import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans8 {
  def main(args: Array[String]): Unit={
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      (1, "2024-07-01"),
      (2, "2024-12-01"),
      (3, "2024-05-01")
    ).toDF("order_id", "order_date")

    orders.select(
      col("order_id"),
      col("order_date"),
      when(col("order_date")==="2024-07-01","Summer")
        .when(col("order_date")==="2024-12-01","Winter")
        .otherwise("other").alias("season")
    ).show()
  }

}

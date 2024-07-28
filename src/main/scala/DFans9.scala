import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
object DFans9 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val sales = List(
      (1, 100),
      (2, 1500),
      (3, 300)
    ).toDF("sale_id", "amount")

    sales.select(
      col("sale_id"),
      col("amount"),
      when(col("amount") > 1000, "20")
        .when(col("amount") >= 200 && col("amount") < 1000, "10")
        .otherwise("0").alias("discount")
    ).show()

  }

}

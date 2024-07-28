import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, datediff, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans15 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val orders = List(
      (1, 5, 100),
      (2, 10, 150),
      (3, 20, 300)
    ).toDF("order_id", "quantity", "total_price")

    orders.select(
      col("order_id"),
      col("quantity"),
      col("total_price"),
      when(col("quantity")<10 && col("total_price")<200,"Small & Cheap")
        .when(col("quantity")>=10 && col("total_price")<200,"Bulk & Discounted")
        .otherwise("Premium Order").alias("order_type")
    ).show()
  }

}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans6 {
  def main(args: Array[String]): Unit={

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val inventory = List(
      (1, 5),
      (2, 15),
      (3, 25)
    ).toDF("item_id", "quantity")

    inventory.select(
      col("item_id"),
      col("quantity"),
      when(col("quantity")<10,"low")
        .when(col("quantity")>10 && col("quantity")<=20,"Medium")
        .otherwise("High").alias("stock_level")
    ).show()
  }

}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object DFans2 {
  def main(args: Array[String]): Unit = {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    val transactions = List(
      (1, 1000),
      (2, 200),
      (3, 5000)
    )
    val df = spark.createDataFrame(transactions).toDF("transaction_id", "amount")
    df.show(false)

    df.select(
      col("transaction_id"),
      col("amount"),
      when(col("amount") > 500 && col("amount") <= 1000, "Medium")
        .when(col("amount") > 1000, "High")
        .otherwise("Low").alias("Value")

    ).show()
  }
}

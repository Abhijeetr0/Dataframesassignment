import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans10 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val logins = List(
      (1, "09:00"),
      (2, "18:30"),
      (3, "14:00")
    ).toDF("login_id", "login_time")
    logins.select(
      col("login_id"),
      col("login_time"),
      when(col("login_time")< "12:00","True")
        .otherwise("False").alias("is_morning")
    ).show()
  }

}

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object DFans1 {
  def main(args: Array[String]): Unit = {
    val sparkconf=new SparkConf()
    sparkconf.set("spark.app.name","spark-program")
    sparkconf.set("spark.master","local[*]")

    val spark=SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()
    import spark.implicits._

    val employees = List(
      (1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22)
    ).toDF("id", "name", "age")

    employees.select(
      col("id"),
      col("name"),
      col("age"),
      when(col("age")>=18,"True")
        .otherwise("False")
        .alias("IsAdult")

    ).show()


  }

}

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

    val employees = List(
      (1, "John", 28),
      (2, "Jane", 35),
      (3, "Doe", 22)
    )
    val df = spark.createDataFrame(employees).toDF("id", "name", "age")
    df.show(false)

    df.select(
      col("id"),
      col("name"),
      col("age"),
      when(col("age")>=18,"Adult")
        .otherwise("Not adult")
        .alias("IsAdult")

    ).show()


  }

}

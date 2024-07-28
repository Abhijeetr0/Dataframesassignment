import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, month, when}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DFans13 {
  def main(args: Array[String]): Unit= {
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "spark-program")
    sparkconf.set("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val documents = List(
      (1, "The quick brown fox"),
      (2, "Lorem ipsum dolor sit amet"),
      (3, "Spark is a unified analytics engine")
    ).toDF("doc_id", "content")

    documents.select(
      col("doc_id"),
      col("content"),
      when(col("content").contains("fox"),"Animal Related")
        .when(col("content").contains("Lorem"),"Placeholder Text")
        .when(col("content").contains("Spark"),"Tech Related")
        .alias("content_category")
    ).show()

  }

}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ScalaMnMCount")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val mnmDF = spark.read.option("header", "true").csv("data/raw/mnm_dataset.csv")

    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDF.show(60, truncate = false)
    println(s"Total Rows = ${countMnMDF.count()}")

    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where($"State" === "CA")
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10, truncate = false)

    spark.stop()
  }
}

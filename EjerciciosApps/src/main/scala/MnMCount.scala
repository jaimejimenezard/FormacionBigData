import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMCount {
  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println("Usage: MnMCount <file>")
      System.exit(1)
    }

    // Crear la SparkSession
    val spark = SparkSession
      .builder()
      .appName("ScalaMnMCount")
      .master("local[*]")
      .getOrCreate()


    import spark.implicits._

    // Archivo CSV de entrada
    val mnmFile = args(0)

    // Leer el CSV con cabecera e inferencia de tipos
    val mnmDF = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Conteo total por estado y color (sumar la columna Count)
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    countMnMDF.show(60, truncate = false)
    println(s"Total Rows = ${countMnMDF.count()}")

    // Conteo solo para California (CA)
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where($"State" === "CA")
      .groupBy("State", "Color")
      .agg(sum("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10, truncate = false)

    // Detener SparkSession
    spark.stop()
  }
}

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object EjerciciosAvanzados {
  def iniciarDF(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv("data/raw/googleapps.csv")
  }

  def ejercicio1(df: DataFrame): DataFrame = {
    println("Life Made WI-FI Touchscreen Photo Frame eliminado.")
    df.filter(col("App") =!= "Life Made WI-FI Touchscreen Photo Frame")
  }

  def ejercicio2(df: DataFrame): DataFrame = {
    println("Nulos de Rating sustituidos.")
    df.na.fill(Map("Rating" -> 0))
  }

  def ejercicio3(df: DataFrame): DataFrame = {
    println("Nulos de Type sustituidos.")
    df.na.fill(Map("Type" -> "Unknown"))
  }

  def ejercicio4(df: DataFrame): DataFrame = {
    println("Columna Varies with Device creada.")
    df.withColumn("Varies with device", when(col("Android Ver") === "Varies with device", true).otherwise(false))
  }

  def ejercicio5(df: DataFrame): DataFrame = {
    println("Columna Frec_Download creada.")
    df.withColumn("Frec_Download",
      expr("CASE WHEN Installs < 50000 THEN 'Low' " +
        "WHEN Installs >= 50000 AND Installs < 1000000 THEN 'Medium'" +
        "WHEN Installs >= 1000000 AND Installs < 50000000 THEN 'High'" +
        "ELSE 'Very high' END")
    )
  }

  def ejercicio6(df: DataFrame): Unit = {
    // 1º apartado
    val highappsDF = df
      .select("*")
      .where(col("Rating") >= 4.5 )
      .where(col("Frec_Download") === "Very high" )
    println("Aplicaciones con frecuencia de descarga y valoración altas.")
    highappsDF.show(60)

    // 2º apartado
    val freeappsDF = df
      .select("*")
      .where(col("Type") === "Free" )
      .where(col("Frec_Download") === "Very high" )
    println("Aplicaciones con frecuencia de descarga muy alta y gratuitas.")
    freeappsDF.show(60)

    // 3º apartado
    val cheapappsDF = df
      .select("*")
      .where(col("Price") <= 13 )
    println("Aplicaciones con precio menor a 13€.")
    cheapappsDF.show(60)
  }

  def ejercicio7(df: DataFrame): Unit = {
    val Array(sampleDF, _) = df.randomSplit(Array(0.1, 0.9), seed = 123)
    sampleDF.show()
  }

}

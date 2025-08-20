import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.StdIn

object Main {
  def main(args: Array[String]) {
    System.setProperty("hadoop.native.io.disable", "true")

    // Crear la SparkSession
    val spark = SparkSession
      .builder()
      .appName("ScalaAvanzados")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    var appsDF = spark.read.option("header", "true").csv("data/raw/googleapps.csv")
    val enunciado =
      """
        | 0: Mostrar dataset.
        | 1: Eliminar la app Life Made WIFI Touchscreen Photo Frame.
        | 2: Sustituir los valores nulos de la columna Rating.
        | 3: Sustituir los valores nulos de la columna Type.
        | 4: Agregar la columna Varies with Device.
        | 5: Agregar la columna Frec_Download.
        | 6: Mostrar consultas.
        | 7: Mostrar una porción del dataset.
        |
        |""".stripMargin

    print(enunciado)
    var ans = 0
    while(ans!=11) {
      ans = StdIn.readInt()
      ans match {
        case 0 => appsDF.show()
        case 1 => appsDF = EjerciciosAvanzados.ejercicio1(appsDF)
        case 2 => appsDF = EjerciciosAvanzados.ejercicio2(appsDF)
        case 3 => appsDF = EjerciciosAvanzados.ejercicio3(appsDF)
        case 4 => appsDF = EjerciciosAvanzados.ejercicio4(appsDF)
        case 5 => appsDF = EjerciciosAvanzados.ejercicio5(appsDF)
        case 6 => EjerciciosAvanzados.ejercicio6(appsDF)
        case 7 => EjerciciosAvanzados.ejercicio7(appsDF)
        case _ => ans=11
      }
    }
    spark.stop()
  }
}
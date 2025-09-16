import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.io.StdIn

object Main {
  def main(args: Array[String]) {
    System.setProperty("hadoop.native.io.disable", "true")

    // Crear la SparkSession
    implicit val spark = SparkSession
      .builder()
      .appName("ScalaAvanzados")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

    val enunciado =
      """
        | 1: Ejercicios avanzados.
        | 2: Prático completo.
        |""".stripMargin

    var ans = 0
    while(ans!=11) {
      print(enunciado)
      ans = StdIn.readInt()
      ans match {

        // Ejercicios avanzados
        case 1 => {
          var appsDF = spark.read.option("header", "true").csv("data/raw/googleapps.csv")

           appsDF.show()
           appsDF = EjerciciosAvanzados.ejercicio1(appsDF)
           appsDF = EjerciciosAvanzados.ejercicio2(appsDF)
           appsDF = EjerciciosAvanzados.ejercicio3(appsDF)
           appsDF = EjerciciosAvanzados.ejercicio4(appsDF)
           appsDF = EjerciciosAvanzados.ejercicio5(appsDF)
           EjerciciosAvanzados.ejercicio6(appsDF)
           EjerciciosAvanzados.ejercicio7(appsDF)
           ans=11
        }

        // Prático Completo
        case 2 => {
          PracticoCompleto.execute()
        }
      }
    }
    spark.stop()
  }
}
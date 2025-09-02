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
    spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")

    val enunciado1 =
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

    val enunciado2 =
      """
        | 1: Crear columna precio total.
        | 2: Estandarizar la columna provincia    .
        | 3: Análisis exploratorio.
        | 4: Consultas con SparkSQL.
        | 5: Análisis avanzado.
        | 6: Optimización de rendimiento.
        | 7: Particionamiento.
        | 8: Uso de RDDs.
        |
        |""".stripMargin

    val enunciado =
      """
        | 1: Ejercicios avanzados.
        | 2: Prático completo.
        |""".stripMargin

    var ans = 0
    var ans1 = 0
    while(ans!=11) {
      print(enunciado)
      ans = StdIn.readInt()
      ans match {

        // Ejercicios avanzados
        case 1 => {
          var appsDF = spark.read.option("header", "true").csv("data/raw/googleapps.csv")
          print(enunciado2)
          ans1 = StdIn.readInt()
          while(ans1!=11) {
            ans1 match {
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
            ans1 = StdIn.readInt()
          }
        }
        // Prático Completo
        case 2 => {
          var pedidosDF = spark.read.option("multiline", "true").json("data/raw/pedidos.json")
          //pedidosDF.show()
          var productosDF = spark.read.option("multiline", "true").json("data/raw/productos.json")
          //productosDF.show()
          var clientesDF = spark.read.option("header","true").csv("data/raw/clientes.csv")
          //clientesDF.show()
          print(enunciado1)

          ans1 = StdIn.readInt()
          while(ans1!=11) {
            ans1 match {
              case 0 => {
                pedidosDF.show()
                productosDF.show()
                clientesDF.show()
              }
              case 1 => pedidosDF = PracticoCompleto.ejercicio1(pedidosDF)
              case 2 => clientesDF = PracticoCompleto.ejercicio2(clientesDF)
              case 3 => PracticoCompleto.ejercicio3(pedidosDF, clientesDF, productosDF)
              case 4 => PracticoCompleto.ejercicio4(spark, pedidosDF, productosDF, clientesDF)
              case 5 => PracticoCompleto.ejercicio5(pedidosDF, clientesDF, productosDF)
              case 6 => PracticoCompleto.ejercicio6(pedidosDF, productosDF)
              case 7 => PracticoCompleto.ejercicio7(pedidosDF)
              case 8 => PracticoCompleto.ejercicio8(pedidosDF, clientesDF)
              case _ => ans=11
            }
            ans1 = StdIn.readInt()
          }
        }

      }
    }
    spark.stop()
  }
}
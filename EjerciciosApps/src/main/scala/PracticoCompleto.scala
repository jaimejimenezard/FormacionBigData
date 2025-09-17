import javassist.runtime.Desc
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object PracticoCompleto  {

  def execute()(implicit spark: SparkSession): Unit = {
    val (pedidosDF, productosDF, clientesDF) = PracticoCompleto.ejercicio1(spark)

    PracticoCompleto.ejercicio2(pedidosDF, productosDF, clientesDF)
    val (pedidosDF_updated, clientesDF_updated) = PracticoCompleto.ejercicio3(pedidosDF, clientesDF)

    PracticoCompleto.ejercicio4(pedidosDF_updated, clientesDF_updated, productosDF)
    PracticoCompleto.ejercicio5(spark, pedidosDF_updated, productosDF, clientesDF_updated)
    PracticoCompleto.ejercicio6(pedidosDF_updated, clientesDF_updated, productosDF)
    PracticoCompleto.ejercicio7(pedidosDF_updated, productosDF)
    PracticoCompleto.ejercicio8(pedidosDF_updated)
    PracticoCompleto.ejercicio9(pedidosDF_updated, clientesDF_updated)
  }

  def ejercicio1(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
    val pedidosDF = spark.read
      .option("multiline", "true")
      .json("data/raw/pedidos.json")

    val productosDF = spark.read
      .option("multiline", "true")
      .json("data/raw/productos.json")

    val clientesDF = spark.read
      .option("header", "true")
      .csv("data/raw/clientes.csv")

    (pedidosDF, productosDF, clientesDF)
  }

  def ejercicio2(ped_df: DataFrame, pro_df: DataFrame, cli_df: DataFrame): Unit = {
    ped_df.show()
    pro_df.show()
    cli_df.show()
  }

  def ejercicio3(ped_df: DataFrame, cli_df: DataFrame) : (DataFrame, DataFrame) = {
    println("Columna precio total creada.")
    val ped_df_new = ped_df.withColumn("precio_total", when(col("descuento").isNotNull, bround(col("cantidad")*col("precio_unitario")*col("descuento"),2))
      .otherwise(bround(col("cantidad")*col("precio_unitario"),2)))

    println("Columna provincia estandarizada.")
    val cli_df_new = cli_df.withColumn("provincia", upper(col("provincia")))

    (ped_df_new, cli_df_new)
  }

  def ejercicio4(ped_df: DataFrame, cli_df: DataFrame, pro_df: DataFrame): Unit = {
    // Determina cuánto ha gastado cada cliente en total.
    val totalGastosDF = ped_df
      .groupBy(col("cliente_id"))
      .agg(bround(sum(col("precio_total")),2).alias("total_pagado"))
    println("Cuanto ha pagado cada cliente en total.")
    totalGastosDF.show(60)
    //Identifica cuál ha sido el producto más solicitado.
    val productPopDF = ped_df
      .groupBy(col("producto_id"))
      .agg(count(col("producto_id")).alias("veces_pedido"))
      .orderBy(desc("veces_pedido"))
    println("Producto más veces pedido.")
    productPopDF.show(1)
    //Media de edad por provincia.
    val mediaEdadDF = cli_df
      .groupBy(col("provincia"))
      .agg(bround(avg(col("edad")),2).alias("media_edad"))
    println("Media de edad por provincia.")
    mediaEdadDF.show(60)
    //Productos vendidos por categoría.
    val prodCategoriaDF = ped_df
      .join(pro_df,pro_df("producto_id")===ped_df("producto_id"))
      .groupBy(pro_df("categoria"))
      .agg(count(pro_df("categoria")).alias("veces_pedida"))
    println("Cantidad de productos vendidos por categoria.")
    prodCategoriaDF.show(60)
  }

  def ejercicio5(spark: SparkSession, ped_df: DataFrame, pro_df: DataFrame, cli_df: DataFrame): Unit = {
    ped_df.createOrReplaceTempView("pedidos")
    ped_df.createOrReplaceTempView("productos")
    cli_df.createOrReplaceTempView("clientes")

    ped_df.createOrReplaceTempView("pedidos")

    val resultado1 = spark.sql("""
      SELECT cliente_id, COUNT(*) AS num_pedidos
      FROM pedidos
      GROUP BY cliente_id
    """)
    resultado1.show()

    val resultado2 = spark.sql("""
      SELECT producto_id, ROUND(SUM(precio_total),2) AS dinero_generado
      FROM pedidos
      GROUP BY producto_id
    """)
    resultado2.show()

    val resultado3 = spark.sql("""
      SELECT provincia, COUNT(DISTINCT clientes.cliente_id) AS clientes
      FROM pedidos INNER JOIN clientes ON pedidos.cliente_id=clientes.cliente_id
      GROUP BY provincia, clientes.cliente_id
      ORDER BY clientes DESC
    """)
    resultado3.show()

    val resultado4 = spark.sql("""
      SELECT count(*) as pedidos, MONTH(fecha) as mes
      FROM pedidos
      group by mes
      ORDER BY mes
    """)

    val resultado5 = spark.sql("""
      SELECT producto_id, COUNT(DISTINCT cliente_id) AS num_clientes
      FROM pedidos
      group by producto_id
      ORDER BY num_clientes DESC
    """)
    resultado5.show()
  }

  def ejercicio6(ped_df: DataFrame, cli_df: DataFrame, pro_df: DataFrame): Unit = {
    // Ejercicio 1
    val ventasPorProducto = ped_df
      .join(pro_df, pro_df("producto_id") === ped_df("producto_id"))
      .groupBy(pro_df("categoria"), pro_df("producto_id"))
      .agg(sum("cantidad").alias("unidades_vendidas"))

    val ventana1 = Window.partitionBy("categoria").orderBy(desc("unidades_vendidas"))
    val ranking1DF = ventasPorProducto
      .withColumn("posicion", rank().over(ventana1))
      .orderBy("categoria", "posicion")

    println("Productos más vendidos por categoría:")
    ranking1DF.show(50, truncate = false)

    ranking1DF.write.format("csv")
      .mode("overwrite")
      .option("compression", "none")
      .option("header", "true")
      .save("data/results/ranking_categoria")

    // ejercicio2
    val ventasPorCliente = ped_df
      .groupBy(
        date_format(col("fecha"), "yyyy-MM").alias("mes"), col("cliente_id")
      )
      .agg(bround(sum("precio_total"),2).alias("dinero_gastado"))

    val ventana2 = Window
      .partitionBy("cliente_id")
      .orderBy("mes")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val acumuladoDF = ventasPorCliente
      .withColumn("dinero_acumulado", bround(sum("dinero_gastado").over(ventana2),2))
      .orderBy("cliente_id", "mes")

    acumuladoDF.show(50, truncate = false)

    acumuladoDF.write.format("csv")
      .mode("overwrite")
      .option("compression", "none")
      .save("data/results/dinero_mes")

    // ejercicio3
    val pedidosCliente = ped_df.select("cliente_id", "pedido_id", "fecha", "precio_total")

    val ventana3 = Window
      .partitionBy("cliente_id")
      .orderBy("fecha")

    val historialDF = pedidosCliente
      .withColumn("n_pedido", row_number().over(ventana3))
      .withColumn("gasto_respecto_anterior", round(col("precio_total") - lag("precio_total", 1).over(ventana3), 2))
      .withColumn("gasto_respecto_posterior", round(col("precio_total") - lead("precio_total", 1).over(ventana3), 2))
      .orderBy("cliente_id", "n_pedido")

    historialDF.show(50, truncate = false)

    historialDF.write.format("csv")
      .mode("overwrite")
      .option("compression", "none")
      .save("data/results/historial_ventas")
  }

  def ejercicio7(ped_df: DataFrame, pro_df: DataFrame): Unit = {
    // Estrategia de almacenamiento
    ped_df.persist(StorageLevel.MEMORY_ONLY)

    // Estrategia lectura desde disco
    ped_df.persist(StorageLevel.DISK_ONLY)

    // Analisis de plan de ejecución
    val ventasPorProducto = ped_df
      .join(pro_df, pro_df("producto_id") === ped_df("producto_id"))
      .groupBy(pro_df("categoria"), pro_df("producto_id"))
      .agg(sum("cantidad").alias("unidades_vendidas"))

    ventasPorProducto.explain(true)



  }

  def ejercicio8(df: DataFrame): Unit = {
    println(s"Número de particiones originales: ${df.rdd.getNumPartitions}")

    val t1 = System.nanoTime()
    val productPopDF = df
      .groupBy(col("producto_id"))
      .agg(count(col("producto_id")).alias("veces_pedido"))
      .orderBy(desc("veces_pedido"))
    productPopDF.show(1)
    val t2 = System.nanoTime()
    println(s"Tiempo original: ${(t2 - t1) / 1e9} segundos")

    val df_repart = df.repartition(8)
    println(s"Número de particiones tras reparticion: ${df_repart.rdd.getNumPartitions}")

    val t3 = System.nanoTime()
    val productPopDF2 = df_repart
      .groupBy(col("producto_id"))
      .agg(count(col("producto_id")).alias("veces_pedido"))
      .orderBy(desc("veces_pedido"))
    productPopDF2.show(1)
    val t4 = System.nanoTime()
    println(s"Tiempo nuevo: ${(t4 - t3) / 1e9} segundos")

  }

  def ejercicio9(ped_df: DataFrame, cli_df: DataFrame): Unit = {
    val df1 = ped_df
      .join(cli_df, ped_df("cliente_id")===cli_df("cliente_id"))
      .withColumn("producto_id", col("producto_id").cast("int"))


    val rdd1 = df1.rdd
    val mapeo1 = rdd1.map(row =>
      (row.getAs[Int]("producto_id"), row.getAs[String]("provincia"))
    )

    mapeo1.take(10).foreach(println)
    print(mapeo1.count())

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._


    val dfFechas = ped_df.withColumn("fecha_date", to_date(col("fecha"), "yyyy-MM-dd"))

    val ventana = Window.partitionBy("cliente_id").orderBy("fecha_date")

    val dfConDiff = dfFechas.withColumn(
      "dias_entre_pedidos", datediff(col("fecha_date"), lag("fecha_date", 1).over(ventana))
    )

    val dfComportamiento = dfConDiff.groupBy("cliente_id")
      .agg(
        count("pedido_id").alias("volumen"),
        bround(avg("dias_entre_pedidos"),2).alias("frecuencia")
      )
      .orderBy("cliente_id")

    dfComportamiento.show()
    }
}
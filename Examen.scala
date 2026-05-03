package examen_estructura

import org.apache.spark.sql.functions.{avg, col, udf}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object Examen {
  def lecturaCSVDF(path: String, delimiter: String = ",")
                  (implicit ss: SparkSession): DataFrame = {
    ss.read.option("header", true).option("delimiter", delimiter).csv(path)
  }

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  def ejercicio1(estudiantes: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Muestra el esquema del DataFrame.
    estudiantes.printSchema()

    //Filtra los estudiantes con una calificación mayor a 8.
    //Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
    val operacionesbasicas = estudiantes
      .filter(col("calificacion") > 8)
      .orderBy(col("calificacion").desc)
      .select(col("nombre"))
    operacionesbasicas

  }

  /** Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números
   */

   def ejercicio2(numeros: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // Define una función que determine si un número es par o impar.
    val ParImpar = udf((n: Int) => {if (n % 2 == 0) "Par" else "Impar"})

    //Aplica esta función a una columna de un DataFrame que contenga una lista de números.
    val aplicarfuncion = numeros
      .withColumn("resultado", ParImpar(col("numero")))
      .select(col("resultado"))
     aplicarfuncion

  }

  /** Ejercicio 3: Joins y agregaciones
   * Pregunta: Dado dos DataFrames,
   * uno con información de estudiantes (id, nombre)
   * y otro con calificaciones (id_estudiante, asignatura, calificacion),
   * realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

  //realiza un join entre ellos
    val promedioestudiantes = calificaciones
     .join(estudiantes, calificaciones("id_estudiante") === estudiantes("id"), "left")
     .groupBy("id", "nombre")

     //calcula el promedio de calificaciones por estudiante
     .agg(avg("calificacion").as("promedio"))
     .select("id", "nombre", "promedio")
    promedioestudiantes
   }

  /** Ejercicio 4: Uso de RDDs
   * Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias
   * de cada palabra.
   */

  def ejercicio4(palabras: List[String])(implicit spark: SparkSession): RDD[(String, Int)] = {
    val sc = spark.sparkContext

    //cuenta la cantidad de ocurrencias de cada palabra
    val ocurrenciaspalabras = sc.parallelize(palabras)
      .map(palabra => (palabra, 1))
      .groupByKey()
      .mapValues(_.size)
  ocurrenciaspalabras
  }

  /**
   * Ejercicio 5: Procesamiento de archivos
   * Pregunta: Carga un archivo CSV que contenga información sobre
   * ventas (id_venta, id_producto, cantidad, precio_unitario)
   * y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */


  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {

//calcular ingreso total (cantidad * precio_unitario) por producto.
    val ingresototal = ventas
      .withColumn("cantidad", col("cantidad").cast("int"))
      .withColumn("precio_unitario", col("precio_unitario").cast("double"))
      .withColumn("ingreso", col("cantidad") * col("precio_unitario"))
      .groupBy("id_producto")
      .agg(functions.sum("ingreso").alias("total_ingreso"))
    ingresototal
    }
}

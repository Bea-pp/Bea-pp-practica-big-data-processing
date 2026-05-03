package examen_estructura

import examen_estructura.Examen._
import org.apache.spark.{SparkContext, rdd}
import org.apache.spark.sql.functions.avg
import utils.TestInit

class examenTest extends TestInit {

  implicit val sc: SparkContext = spark.sparkContext
  import spark.implicits._

  "Ejercicio 1" should "Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente" in {

    val estudiantes = Seq(
      ("Ana", 23, 9.0),
      ("Luis", 21, 7.5),
      ("Pedro", 22, 8.5),
      ("Maria", 20, 9.5)
    ).toDF("nombre", "edad", "calificacion")

    val out = ejercicio1(estudiantes).collect().map(_.getString(0))
    out shouldBe List("Maria", "Ana", "Pedro")

    val resDF = ejercicio1(estudiantes)
    resDF.show()
  }

  "Ejercicio 2" should "Devuelve los datos paritarios" in {

    val numeros = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("numero")

    val out = ejercicio2(numeros).collect().map(_.getString(0))
    out shouldBe List("Impar", "Par", "Impar", "Par", "Impar", "Par", "Impar", "Par", "Impar", "Par")

    val resDF = ejercicio2(numeros)
    resDF.show()
  }

  "Ejercicio 3" should "calcula el promedio de calificaciones por estudiante" in {

    val estudiantes = Seq(
      (1, "Ana"),
      (2, "Luis"),
      (3, "Pedro"),
      (4, "Maria")
    ).toDF("id", "nombre")

    val calificaciones = Seq(
      (1, "Matematicas", 9.0),
      (1, "Historia", 8.5),
      (2, "Matematicas", 7.5),
      (3, "Historia", 8.0),
      (4, "Matematicas", 9.5),
      (4, "Historia", 9.0)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    val out = ejercicio3(estudiantes, calificaciones).collect().map(x => (x.get(0), x.get(1), x.get(2)))
    out shouldBe List((1, "Ana", 8.75), (2, "Luis", 7.5), (3, "Pedro", 8.0), (4, "Maria", 9.25))

    val resDF = ejercicio3(estudiantes, calificaciones)
    resDF.show()
  }

  "Ejercicio 4" should "cuenta la cantidad de ocurrencias de cada palabra" in {

    val palabras = List("spark", "hadoop", "spark", "hive", "spark", "hadoop", "hive")

    val out = ejercicio4(palabras).collect()
    out.sorted shouldBe Array(("hadoop", 2), ("hive", 2), ("spark", 3))

    val rdd = ejercicio4(palabras)
    out.foreach(println)
  }

  "Ejercicio 5" should "calcula el ingreso total" in {

    val path = "C:\\KEEPCODING\\VISUAL STUDIO CODE\\BOOTCAMP\\ABR26 BIG DATA PROCESSING\\BD_PROCESSING_15\\DB_PROCESSING_15\\src\\main\\scala\\examen_estructura\\ventas.csv"
    val ventas = examen_estructura.Examen.lecturaCSVDF(path)

    val out = ejercicio5(ventas).collect().map(x => (x.getString(0).toInt, x.getDouble(1)))
    out shouldBe List(
      (101, 460.0), (107, 396.0), (110, 494.0),
      (104, 800.0), (102, 405.0), (103, 280.0),
      (108, 486.0), (106, 425.0), (105, 570.0), (109, 540.0)
    )

    val resDF = ejercicio5(ventas)
    resDF.show()
  }
}


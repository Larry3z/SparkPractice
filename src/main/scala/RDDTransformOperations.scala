import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}


object RDDTransformOperations {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("file/log4j.properties")
    val conf = new SparkConf().setAppName("RDDTransformOperations")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.parallelize(List("Hello world","Hello scala"))

    //map
    lines.map(line => line.toUpperCase).foreach(println)
    //flatMap
    lines.flatMap(line => line.split(" ")).foreach(println)
    //filter
    lines.flatMap(line => line.split(" ")).filter(x => x != "Hello")
      .foreach(println)
    //distinct
    lines.flatMap(line => line.split(" ")).distinct().foreach(println)

    val lines2 = sc.parallelize(List("hello union","Hello world","test"))

    // union
    lines.union(lines2).foreach(println)
    // intersection
    lines.intersection(lines2).foreach(println)
    //subtract
    lines2.subtract(lines).foreach(println)
    //cartesian
    lines.cartesian(lines2).foreach(println)

  }
}

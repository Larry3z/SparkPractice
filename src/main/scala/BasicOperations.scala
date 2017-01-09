import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}

/**

  * RDD基本的概念
  */
object BasicOperations {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure("file/log4j.properties")
    val conf = new SparkConf().setAppName("BasicOperations").setMaster("local")
    val sc = new SparkContext(conf)

    // spark中创建RDD的两种方法，一种以外部文件，另一种以内部数据
    val outerRDDs = sc.textFile("file/README.md")
    val innerRDDs = sc.parallelize(List("hello world", "hello scala"))

    //RDD两种操作transform和action，惰性求值
    val ss = innerRDDs.map(_.contains()).cache()
    innerRDDs.map(x => x.toUpperCase).foreach(println)
    outerRDDs.map(x => x.toUpperCase).filter(x => !x.contains("SPARK") && x != "").foreach(println)
  }
}
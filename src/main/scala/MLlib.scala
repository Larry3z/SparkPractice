import org.apache.log4j.PropertyConfigurator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

/**
  * 机器学习这块只能先做到理解算法，会调用算法包。
  * 这个垃圾邮件过滤的程序就是练习，由于中文涉及到如何分词的问题，所以这块先用英文做，以后在考虑中文的问题
  */
object MLlib {
  def main(args: Array[String]) {
    PropertyConfigurator.configure("file/log4j.properties")
    val conf = new SparkConf().setAppName("MLlib").setMaster("local")
    val sc = new SparkContext(conf)

    //读取垃圾邮件和非垃圾邮件的训练集
    val spam = sc.textFile("file/spam.txt")
    val ham = sc.textFile("file/ham.txt")

    // HashingTF 从文本数据构建词频的特征向量。包含100个特征features的向量vectors
    val tf = new HashingTF(numFeatures = 100)
    // 每个邮件都被切分成单词，每个单词都被映射成特征
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val hamFeatures = ham.map(email => tf.transform(email.split(" ")))

    // 为垃圾邮件和正常邮件创建LabeledPoint数据集
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = hamFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // 不断迭代所以缓存训练数据集

    // 使用SGD Stochastic Grandient Descent随机梯度下降法实现逻辑回归
    val lrLearner = new LogisticRegressionWithSGD()
    // 在训练数据集上运行算法
    val model = lrLearner.run(trainingData)

    // 测试数据要和训练数据的特征集一致
    val posTestExample = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
    val negTestExample = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
    println(s"Prediction for positive test example: ${model.predict(posTestExample)}")
    println(s"Prediction for negative test example: ${model.predict(negTestExample)}")

    Thread.sleep(50000)

    sc.stop()
  }
}

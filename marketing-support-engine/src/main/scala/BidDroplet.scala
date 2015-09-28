import java.io.File

import hex.deeplearning.{DeepLearning, DeepLearningParameters}
import org.apache.spark.h2o.{H2OContext, StringHolder}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import water.fvec.H2OFrame

object BidDroplet {
  def main(args: Array[String]): Unit = {
    // Create Spark Context
    val conf = new SparkConf()
      .setAppName("Bid Droplet")
      .setIfMissing("spark.master", sys.env.getOrElse("spark.master", "local"))
    val sc = new SparkContext(conf)

    // Create H2O Context
    val h2oContext = new H2OContext(sc).start()
    import h2oContext._

    // Register file to be available on all nodes
    sc.addFile(new File("/mnt/data/workspace/laughing-octo-sansa/data/final_train.txt").getAbsolutePath)

    // Load data and parse it via h2o parser
    val bidTable = new H2OFrame(new File(SparkFiles.get("final_train.txt")))

    println(s"\n===> Number of all bid log items via H2O#Frame#count: ${bidTable.numRows()}\n")

    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = bidTable
    dlParams._response_column = 'sitelead

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    // Make prediction on train data
    val predict = dlModel.score(bidTable)('predict)

    println("\n====> Making prediction with help of DeepLearning model\n")
    val predictionsFromModel = asRDD[StringHolder](predict).collect.map ( _.result.getOrElse("NaN") )
    println(predictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

    // Compute number of mispredictions with help of Spark API
    val trainRDD = asRDD[StringHolder](bidTable('sitelead))
    val predictRDD = asRDD[StringHolder](predict)

    // Make sure that both RDDs has the same number of elements
    assert(trainRDD.count() == predictRDD.count)
    val numMispredictions = trainRDD.zip(predictRDD).filter( i => {
      val act = i._1
      val pred = i._2
      act.result != pred.result
    }).collect()

    println(
      s"""
         |Number of mispredictions: ${numMispredictions.length}
          |
          |Mispredictions:
          |
          |actual X predicted
          |------------------
          |${numMispredictions.map(i => i._1.result.get + " X " + i._2.result.get).mkString("\n")}
       """.stripMargin)
  }
}

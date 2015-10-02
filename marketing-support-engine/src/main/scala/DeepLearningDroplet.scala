import java.io.{File, FileOutputStream, ObjectOutputStream}

import hex.deeplearning.{DeepLearning, DeepLearningParameters}
import org.apache.spark.h2o.{DoubleHolder, H2OContext, StringHolder}
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import water.fvec.H2OFrame

object DeepLearningDroplet {
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
    sc.addFile(new File("/media/sf_Download/data/iPinYou/ipinyou.contest.dataset_unpacked/training2nd/model/dlmodel4_files/final_train.txt").getAbsolutePath)

    // Load data and parse it via h2o parser
    val bidTable = new H2OFrame(new File(SparkFiles.get("final_train.txt")))

    println(s"\n===> Number of all bid log items via H2O#Frame#count: ${bidTable.numRows()}\n")

    // Configure Deep Learning algorithm
    val dlParams = new DeepLearningParameters()
    dlParams._train = bidTable
    dlParams._epochs = 5
    dlParams._response_column = 'sitelead

    val dl = new DeepLearning(dlParams)
    val dlModel = dl.trainModel.get

    val fos = new FileOutputStream("/media/sf_Download/data/iPinYou/ipinyou.contest.dataset_unpacked/training2nd/model/mors_folder/model.file")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(dlModel)
    oos.close()

//    def exportH2OModel(exportModel: Model[_ <: Model, _ <: Model.Parameters, _ <: Model.Output], dir: String) {
//      val model: Model[_ <: Model, _ <: Model.Parameters, _ <: Model.Output] = Handler.getFromDKV("model_id", exportModel._key, classOf[Model[_ <: Model[M, P, O], _ <: Model.Parameters, _ <: Model.Output]])
//
//      val keysToExport: List[Key[_ <: Keyed[_]]] = new LinkedList[Key[_ <: Keyed[_]]]
//      keysToExport.add(model._key)
//      keysToExport.addAll(model.getPublishedKeys)
//      try {
//        new ObjectTreeBinarySerializer().save(keysToExport, FileUtils.getURI(dir))
//      }
//      catch {
//        case e: IOException => {
//          throw new H2OIllegalArgumentException("dir", "exportModel", e)
//        }
//      }
//    }

    // Make prediction on train data
    val predict = dlModel.score(bidTable)('predict)

    println("\n====> Making prediction with help of DeepLearning model\n")
    val strPredictionsFromModel = asRDD[StringHolder](predict).collect.map(_.result.getOrElse("NaN") )
    val doublePredictionsFromModel = asRDD[DoubleHolder](predict).collect.map(_.result.getOrElse("NaN") )
    println(strPredictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))
    println(doublePredictionsFromModel.mkString("\n===> Model predictions: ", ", ", ", ...\n"))

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


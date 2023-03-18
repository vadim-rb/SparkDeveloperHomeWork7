import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Iris extends  App {


    val conf = new SparkConf().setAppName("IrisIrisFisher")
      .set("spark.master", "local[2]")
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "file:///home/vadim/MyExp/spark-logs/event")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val lines = sc.textFile("src/main/resources/data/iris.data")

    val rdd: RDD[LabeledPoint] = lines filter (line => line.nonEmpty) map { line =>
      val v = line.split(",")
      LabeledPoint(v(4) match {
        case "Iris-setosa" => 0.0
        case "Iris-versicolor" => 1.0
        case "Iris-virginica" => 2.0
        case _ => -1.0 // should never occur
      }, Vectors.dense(v(0).toDouble, v(1).toDouble, v(2).toDouble, v(3).toDouble))
    }

    val Array(data, test) = rdd.randomSplit(Array(0.8,0.2))

    // Count
    println("#data = "+data.count)
    println("#test = "+test.count)
    println(test.collect.mkString("Array(", ", ", ")"))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(data, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    test.foreach{
      element =>
      println(element.label + "=" + model.predict(element.features))
    }

    val modelPath = "./src/main/resources/modelLast/"
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(modelPath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

    model
      .save(sc,modelPath)


    val modelLoaded = DecisionTreeModel.load(sc, modelPath)
    println(s" Predict label on random is ${modelLoaded.predict(Vectors.dense("4.7".toDouble, "3".toDouble, "1".toDouble, "0.5".toDouble))}")

  //println("Learned classification tree model:\n" + model.toDebugString)

    

}

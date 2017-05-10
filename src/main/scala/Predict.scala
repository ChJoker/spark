import org.apache.spark.sql.SparkSession

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree

object Predict {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    import spark.implicits._
    val flightData = spark.read.option("header", "true").csv("/Users/apple/.ivy/jars/design/2007.csv")
    val tmpFlightDataRDD = flightData.map(
      row => row(2).toString + "," // dayOfMonth
        + row(3).toString + "," // dayOfWeek
        + row(5).toString + "," // CRSDepTime
        + row(7).toString + "," // CRSArrTime
        + row(8).toString + "," // UniqueCarrier
        + row(12).toString + "," // CRSElapsedTime
        + row(16).toString + "," // Origin
        + row(17).toString + "," // Dest
        + row(14).toString + "," // ArrDelay
        + row(15).toString) // DepDelay
    val flightRDD = tmpFlightDataRDD.map(parseFields)
    var uniqueCarrier_id: Int = 0

    var mCarrier: Map[String, Int] = Map()
    flightRDD.map(flight => flight.uniqueCarrier).distinct.collect.foreach(x => {
      mCarrier += (x -> uniqueCarrier_id);
      uniqueCarrier_id += 1
    })

    var origin_id: Int = 0
    var mOrigin: Map[String, Int] = Map()
    flightRDD.map(flight => flight.origin).distinct.collect.foreach(x => {
      mOrigin += (x -> origin_id);
      origin_id += 1
    })

    var dest_id: Int = 0
    var mDest: Map[String, Int] = Map()
    flightRDD.map(flight => flight.dest).distinct.collect.foreach(x => {
      mDest += (x -> dest_id);
      dest_id += 1
    })

    val featuredRDD = flightRDD.map(flight => {
      val vDayOfMonth = flight.dayOfMonth - 1
      val vDayOfWeek = flight.dayOfWeek - 1
      val vCRSDepTime = flight.crsDepTime
      val vCRSArrTime = flight.crsArrTime
      val vCarrierID = mCarrier(flight.uniqueCarrier)
      val vCRSElapsedTime = flight.crsElapsedTime
      val vOriginID = mOrigin(flight.origin)
      val vDestID = mDest(flight.dest)
      val vDelayFlag = flight.delayFlag

      Array(vDelayFlag.toDouble, vDayOfMonth.toDouble, vDayOfWeek.toDouble, vCRSDepTime.toDouble, vCRSArrTime.toDouble, vCarrierID.toDouble, vCRSElapsedTime.toDouble, vOriginID.toDouble, vDestID.toDouble)
    })

    val LabeledRDD = featuredRDD.map(x => LabeledPoint(x(0), Vectors.dense(x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8))))

    val notDelayedFlights = LabeledRDD.filter(x => x.label == 0).randomSplit(Array(0.8, 0.2))(0)
    val delayedFlights = LabeledRDD.filter(x => x.label == 1)
    val tmpTTData = notDelayedFlights.union(delayedFlights)
    val TTData = tmpTTData.randomSplit(Array(0.7, 0.3))
    val trainingData = TTData(0)
    val testData = TTData(1)

    // 仿照 API 文档中的提示，构造各项参数
    var paramCateFeaturesInfo = Map[Int, Int]()

    // 第一个特征信息：下标为 0 ，表示 dayOfMonth 有 0 到 30 的取值。
    paramCateFeaturesInfo += (0 -> 31)

    // 第二个特征信息：下标为 1 ，表示 dayOfWeek 有 0 到 6 的取值。
    paramCateFeaturesInfo += (1 -> 7)

    // 第三、四个特征是出发和抵达时间，这里我们不会用到，故省略。

    // 第五个特征信息：下标为 4 ，表示 uniqueCarrier 的所有取值。
    paramCateFeaturesInfo += (4 -> mCarrier.size)

    // 第六个特征信息为飞行时间，同样忽略。

    // 第七个特征信息：下标为 6 ，表示 origin 的所有取值。
    paramCateFeaturesInfo += (6 -> mOrigin.size)

    // 第八个特征信息：下标为 7， 表示 dest 的所有取值。
    paramCateFeaturesInfo += (7 -> mDest.size)

    // 分类的数量为 2，代表已延误航班和未延误航班。
    val paramNumClasses = 2

    // 下面的参数设置为经验值
    val paramMaxDepth = 9
    val paramMaxBins = 7000
    val paramImpurity = "gini"

    val flightDelayModel = DecisionTree.trainClassifier(trainingData.rdd, paramNumClasses, paramCateFeaturesInfo, paramImpurity, paramMaxDepth, paramMaxBins)

    val predictResult = testData.rdd.map{flight =>
      val tmpPredictResult = flightDelayModel.predict(flight.features)
      (flight.label, tmpPredictResult)
    }

    val numOfCorrectPrediction = predictResult.filter{case (label, result) => (label == result)}.count()
    val predictAccuracy = numOfCorrectPrediction/testData.count().toDouble
    println(predictAccuracy)

  }

  def parseFields(input: String): Flight = {
    val line = input.split(",")

    // 针对可能出现的无效值“NA”进行过滤
    var dayOfMonth = 0
    if (line(0) != "NA") {
      dayOfMonth = line(0).toInt
    }
    var dayOfWeek = 0
    if (line(1) != "NA") {
      dayOfWeek = line(1).toInt
    }

    var crsDepTime = 0.0
    if (line(2) != "NA") {
      crsDepTime = line(2).toDouble
    }

    var crsArrTime = 0.0
    if (line(3) != "NA") {
      crsArrTime = line(3).toDouble
    }

    var crsElapsedTime = 0.0
    if (line(5) != "NA") {
      crsElapsedTime = line(5).toDouble
    }

    var arrDelay = 0
    if (line(8) != "NA") {
      arrDelay = line(8).toInt
    }
    var depDelay = 0
    if (line(9) != "NA") {
      depDelay = line(9).toInt
    }

    // 根据延迟时间决定延迟标志是否为1
    var delayFlag = 0
    if (arrDelay > 30 || depDelay > 30) {
      delayFlag = 1
    }
    Flight(dayOfMonth, dayOfWeek, crsDepTime, crsArrTime, line(4), crsElapsedTime, line(6), line(7), arrDelay, depDelay, delayFlag)
  }

}


case class Flight(dayOfMonth: Int, dayOfWeek: Int, crsDepTime: Double, crsArrTime: Double, uniqueCarrier: String, crsElapsedTime: Double, origin: String, dest: String, arrDelay: Int, depDelay: Int, delayFlag: Int)

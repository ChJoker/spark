import org.apache.spark.sql.SparkSession


object Analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()
    val flightData = spark.read.option("header", "true").csv("/Users/apple/.ivy/jars/design/2007.csv")
    flightData.createOrReplaceTempView("flights")

    val airportData = spark.read.option("header", "true").csv("/Users/apple/.ivy/jars/design/airports-csv.csv")
    airportData.createOrReplaceTempView("airports")

    // 每天航班最繁忙的时间段是哪些？
    val FlightNum_group_hours = spark.sql("SELECT count(*),floor(DepTime/100) FROM flights GROUP BY floor(DepTime/100) ORDER BY floor(DepTime/100) ")
    FlightNum_group_hours.write.csv("/Users/apple/Desktop/idea/spark/result/FlightNum_group_hours")

    val FlightNum_group_origin_hours = spark.sql("" +
      " SELECT count(*),origin,floor(DepTime/100)" +
      " FROM flights" +
      " GROUP BY origin, floor(DepTime/100)" +
      " ORDER BY origin, floor(DepTime/100)")
    FlightNum_group_origin_hours.write.csv("/Users/apple/Desktop/idea/spark/result/FlightNum_group_origin_hours")

    val FlightNum_group_day_hours = spark.sql("" +
      " SELECT count(*),printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)),floor(DepTime/100)" +
      " FROM flights" +
      " GROUP BY printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)), floor(DepTime/100)" +
      " ORDER BY printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)), floor(DepTime/100)")
    FlightNum_group_day_hours.write.csv("/Users/apple/Desktop/idea/spark/result/FlightNum_group_day_hours")
    val FlightNum_group_month_hours = spark.sql("" +
      " SELECT count(*),printf('%04d%02d',cast(Year as int), cast(Month as int)),floor(DepTime/100)" +
      " FROM flights" +
      " GROUP BY printf('%04d%02d',cast(Year as int), cast(Month as int)), floor(DepTime/100)" +
      " ORDER BY printf('%04d%02d',cast(Year as int), cast(Month as int)), floor(DepTime/100)")
    FlightNum_group_month_hours.write.csv("/Users/apple/Desktop/idea/spark/result/FlightNum_group_month_hours")

    // 出发延误的重灾区都有哪些？
    val delay_group_origin = spark.sql("" +
      " SELECT sum(DepDelay)/count(*) as percentage,origin" +
      " FROM flights" +
      " WHERE Cancelled = 0" +
      " GROUP BY origin" +
      " ORDER BY percentage DESC")
    delay_group_origin.write.csv("/Users/apple/Desktop/idea/spark/result/delay_group_origin")

    // 飞哪最准时
    val not_delay_group_state = spark.sql("" +
      " SELECT sum(ontimeArr)/sum(allArr) as percentage, state" +
      " FROM (SELECT Dest, SUM(if(ArrDelay == 1, 0, 1)) as ontimeArr, COUNT(*) as allArr FROM flights WHERE Cancelled = 0 GROUP BY Dest) a" +
      " JOIN airports b ON a.Dest = b.iata" +
      " GROUP BY state" +
      " ORDER BY percentage DESC")
    not_delay_group_state.write.csv("/Users/apple/Desktop/idea/spark/result/not_delay_group_state")

    // 什么时候是一年中最好的一天的飞行时间，以尽量减少延误？
    val not_delay_group_day = spark.sql("" +
      " SELECT sum(if(DepDelay == 1, 0,1))/count(*) as percentage,printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int))" +
      " FROM flights" +
      " WHERE Cancelled = 0" +
      " GROUP BY printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int))" +
      " ORDER BY percentage DESC")
    not_delay_group_day.write.csv("/Users/apple/Desktop/idea/spark/result/not_delay_group_day")

    // 不同地点之间飞行的人数会随时间而变化吗？
    val flight_count_group_origin_day = spark.sql("" +
      " SELECT count(*) as percentage,printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)) as date, origin" +
      " FROM flights" +
      " GROUP BY printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)), origin" +
      " ORDER BY date")
    flight_count_group_origin_day.write.csv("/Users/apple/Desktop/idea/spark/result/flight_count_group_origin_day")
    val flight_count_group_Dest_day = spark.sql("" +
      " SELECT count(*) as percentage,printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)) as date, dest" +
      " FROM flights" +
      " GROUP BY printf('%04d%02d%02d',cast(Year as int), cast(Month as int), cast(DayofMonth as int)), dest" +
      " ORDER BY date")
    flight_count_group_Dest_day.write.csv("/Users/apple/Desktop/idea/spark/result/flight_count_group_Dest_day")

    // 天气如何影响飞机延误？
    val weather_delay_group_origin = spark.sql("" +
      " SELECT sum(if(WeatherDelay != 'NA', 1, 0))/count(*) as percentage, origin" +
      " FROM flights" +
      " WHERE DepDelay = 1" +
      " GROUP BY origin" +
      " ORDER BY percentage DESC")
    weather_delay_group_origin.write.csv("/Users/apple/Desktop/idea/spark/result/weather_delay_group_origin")

    // 级联故障
    val delay_relate_group_airport = spark.sql("" +
      " SELECT delaySum/lDelaySum as percentage, Origin" +
      " FROM (SELECT year, month, dayofMonth, dest, COUNT(*) as delaySum FROM flights WHERE ArrDelay = 1 GROUP BY Dest, year, month, dayofMonth) a" +
      " JOIN (SELECT year, month, dayofMonth, Origin, COUNT(*) as lDelaySum FROM flights WHERE LateAircraftDelay != 'NA' GROUP BY Origin, year, month, dayofMonth) b ON a.Dest = b.Origin and a.year = b.year and a.month = b.month and a.dayofMonth = b.dayofMonth" +
      " ORDER BY percentage DESC")
    delay_relate_group_airport.write.csv("/Users/apple/Desktop/idea/spark/result/delay_relate_group_airport")

//    val r = delay_relate_group_airport.take(10)
    //    delay_relate_group_airport.foreach(t => println(t))
  }
}

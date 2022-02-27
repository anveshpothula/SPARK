import org.apache.spark.SparkContext

object movieRatings extends App {

  val sc = new SparkContext(master = "local[*]", appName = "topRated")

  val ratingsRDD = sc.textFile("D:\\GDrive\\BIG DATA\\WEEK 11\\datasets\\ratings-201019-002101.dat")
  .map(x=>(x.split("::")(1),x.split("::")(2).toInt))
  // (movieID,rating)

  val moviesRDD = sc.textFile("D:\\GDrive\\BIG DATA\\WEEK 11\\datasets\\movies-201019-002101.dat")
    .map(x=>(x.split("::")(0),x.split("::")(1)))
  //(movieID,name)

  val baseRDD = ratingsRDD.join(moviesRDD)
  //movieID,rating,name
  val rateCount = ratingsRDD.mapValues(x=>(x,1))
  val aggRate = rateCount.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val avgRate = aggRate.mapValues(x=>((x._1.toFloat/x._2), x._2))
  //movieID,avgRating,numberOfRatings
 val filteredRDD = avgRate.filter(x=>x._2._2 >=100)

  val outputRDD = filteredRDD.join(moviesRDD).map(x=>x._2).map(x=>(x._2,x._1._1)).filter(x=>x._2 >=4.5).sortBy(x=>x._2,false)
    .collect.foreach(println)
}

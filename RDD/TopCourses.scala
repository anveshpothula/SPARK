import org.apache.spark.SparkContext

object topCourses extends App {

 val sc = new SparkContext(master = "local[*]", appName = "Rater")
  val chaptersRDD = sc.textFile("D:\\GDrive\\BIG DATA\\WEEK 10\\assignment\\chapters-201108-004545.csv")
    .map(x=>x.split(","))
    .map(x=>(x(0).toInt,x(1).toInt))
  //chaptersRDD.collect.foreach(println)
 //(chapterID, courseID)

  val viewsRDD = sc.textFile("D:\\GDrive\\BIG DATA\\WEEK 10\\assignment\\views*.csv")
    .map(x=>x.split(","))
    .map(x=>(x(0).toInt,x(1).toInt))
  //println(viewsRDD.count())
 //(userId, chapterId)

  val titlesRDD = sc.textFile("D:\\GDrive\\BIG DATA\\WEEK 10\\assignment\\titles-201108-004545.csv")
    .map(x=>x.split(","))
    .map(x=>(x(0).toInt,x(1)))


  //Question 1 - Chapters per course
  //InputRDD (chapterID, courseID)
  val chapterCountPerCourse = chaptersRDD.map(x=>(x._2,1)).reduceByKey((x,y)=>x+y).sortBy(x=>x._2)

  //Question 2 - Rank the courses based on views
  // Views Unique (chapterId,userId)


val viewsUnique = viewsRDD.distinct()
 val viewsFlipped =viewsUnique.map(x=>(x._2,x._1))

 val viewChapters = chaptersRDD.join(viewsFlipped)
 //viewChapters.collect.foreach(println)
 //(chapterID,(CourseID,UserID)
val viewUsers = viewChapters.map(x=>(x._2._1,x._2._2))
 //(CourseID,userID)
 val courseViewsPerUser = viewUsers.map(x=>(x,1)).reduceByKey(_+_)
 //(CourseID,UserID),Views). Course ID and UserID is a unique combination.
 val courseVpuFinal = courseViewsPerUser.map(x=>(x._1._1,x._2))
 val rankPrelim = courseVpuFinal.join(chapterCountPerCourse)
 //course ID, (views,chaptercount)
 val rankMapped = rankPrelim.map(x=>(x._1,(x._2._1.toFloat/x._2._2)))

 val scoreMapped = rankMapped.mapValues(x=>{
  if(x>=0.9)10
  else if (x>0.5 && x<0.9)4
  else if (x>0.25 && x<0.5)2
  else 0
 } )
 val scoreFinal = scoreMapped.reduceByKey(_+_)
 val courseScore = scoreFinal.join(titlesRDD).map(x=>(x._2._2,x._2._1 )).sortBy(x=>x._2,false)
 courseScore.take(10).foreach(println)
}

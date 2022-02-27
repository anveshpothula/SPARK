import org.apache.spark.sql.SparkSession

object dataFramesEg extends App {

val spark = SparkSession.builder().appName("myApp1").master("local[*]").getOrCreate()

}

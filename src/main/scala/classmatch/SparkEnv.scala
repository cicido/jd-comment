package classmatch

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by duanxiping on 2017/2/6.
  */
class SparkEnv(appname:String) {
  /*
   System.setProperty("user.name", "ad_recommend")
   System.setProperty("HADOOP_USER_NAME", "ad_recommend")
   */
  val sparkConf: SparkConf = new SparkConf().setAppName(appname)
  val sc = new SparkContext(sparkConf)
  sc.hadoopConfiguration.set("mapred.output.compress", "false")
  val hiveContext = new HiveContext(sc)
  /*
  hiveContext.setConf("mapred.output.compress", "false")
  hiveContext.setConf("hive.exec.compress.output", "false")
  hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
  */
  val sqlContext = new SQLContext(sc)
}

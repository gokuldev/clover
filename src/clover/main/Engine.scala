package clover.main

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

class Engine {
  println("Clover Engine POC")  
}

object Engine {
  def main(args: Array[String]): Unit = {
    
    // set logger info
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    // initiate Spark session
    val spark: SparkSession = getSparkSession
       
    // TODO: pass as args
    val uatFilePath = "C:\\Users\\Gokul\\Desktop\\FinWorkArea\\clover\\file1_UAT_mocked.txt"
    val prdFilePath = "C:\\Users\\Gokul\\Desktop\\FinWorkArea\\clover\\file1_PRD_mocked.txt"
    val schemaString = "custId,custFirstName,custLastName,custTransId,custStockName,custCredits,custPremiumPercent"
    val keyColString = "custId,custTransId"
    val outcomePath = "C:\\Users\\Gokul\\Desktop\\FinWorkArea\\clover\\outcome"
    val engine: EngineImpl = new EngineImpl(uatFilePath, prdFilePath,schemaString,keyColString,outcomePath)
    engine.execute     // trigger engine

  }

 def getSparkSession(): SparkSession = {
    return SparkSession
      .builder()
      .appName("Clover Engine")
      .master("local")
      
      .getOrCreate()
  }

}
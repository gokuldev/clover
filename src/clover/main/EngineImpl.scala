package clover.main

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.{ functions => f }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.DataTypes

class EngineImpl(uatFilePath: String, prodFilePath: String, schemaString:String, keyColString:String,outcomePath:String) extends Serializable{

  // setting up few required params that will be moved to property file later TODO:
  val schemaStringArray: Array[String] = schemaString.split(",")
  val keyColumnsArray: Array[String] = keyColString.split(",")
  val toleranceRange: Integer = 2

  def execute(): Unit = {

    // load the UAT file
    val uatDF: Dataset[Row] = readAndParseFile(uatFilePath)

    // load the prod file
    val prodDF: Dataset[Row] = readAndParseFile(prodFilePath)

    // Join the files
    val joinedDF: Dataset[Row] = performJoin(uatDF, prodDF)

    // define udf and register
    Engine.getSparkSession().udf.register("compareCols", compare(_:String,_:String))
    
    // Get outcome by doing comparison
    val outcomeDF: Dataset[Row] = extractErrors(joinedDF)

    // get the output
    outcomeDF.coalesce(1).write.format("csv").option("header", "true").csv(outcomePath)

}

  def readAndParseFile(filePath: String): Dataset[Row] = {

    // define schema from the obtained Schema String
    val schema: StructType = StructType(schemaStringArray.map(colName => StructField(colName, StringType, nullable = true)))

    // Read the file from the path and create a RDD
    val fileRDD: RDD[String] =
      Engine.getSparkSession().sparkContext.textFile(filePath, 10)

    // Convert RDD[String] to RDD[Row]
    val fileFieldsRDD: RDD[Row] = fileRDD.map(x => x.split("\\|")).map(fields => Row.fromSeq(fields))

    // Return the dataframe
    val fileDF: Dataset[Row] = Engine.getSparkSession().createDataFrame(fileFieldsRDD, schema)

    // Apply some common cleaning functions
    val fileCleanedDF: Dataset[Row] = fileDF.select(fileDF.columns.map(x => f.lower(f.trim(f.coalesce(f.col(x), f.lit("")))).alias(x)): _*)

    return fileCleanedDF
  }

  def performJoin(uatDF: Dataset[Row], prodDF: Dataset[Row]): Dataset[Row] = {

    // rename non-key columns in uatDF
    val uatRenamedDF = uatDF
      .select(uatDF.columns.map(x => if (keyColumnsArray.contains(x)) f.col(x).alias(x) else f.col(x).alias("uat" + x)): _*)

    // rename non-key columns in prodDF
    val prodRenamedDF: Dataset[Row] = prodDF
      .select(prodDF.columns.map(x => if (keyColumnsArray.contains(x)) f.col(x).alias(x) else f.col(x).alias("prod" + x)): _*)

    //perform joined outcome
    return uatRenamedDF.join(prodRenamedDF, keyColumnsArray, "inner")
  }

  def extractErrors(joinedDF: Dataset[Row]): Dataset[Row] = {
    val colsForCompare: Array[String] = schemaStringArray.diff(keyColumnsArray)
    
    val outcomeDF:Dataset[Row] = colsForCompare
                                    .foldLeft(joinedDF)((joinedDF, x) => 
                                      (joinedDF.withColumn("outcome" + x, f.callUDF("compareCols", f.col("uat"+x), f.col("prod"+x)))))
    
    return outcomeDF;
  }

  
  def compare(firstCol: String, secondCol: String): String = {
    var result: String = null
    if (firstCol.equals(secondCol))
      result = "ok"
    else
      result = "not-ok"
    return result
  }

}
package com.cloud.MapReduce.Jobs

import com.cloud.MapReduce.Model.OutputModel
import com.cloud.MapReduce.Model.QueryModel
import com.cloud.MapReduce.Data.DataExtractor;
import com.cloud.MapReduce.GlobalVariables

import org.apache.hadoop.fs.Path
import org.apache.hadoop.util.Time
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkJob {

  /**
   * Method for executing Spark job
   *
   * @param parsedQuery
   * @param groupByOutput
   */
  /**
 * @param parsedQuery
 * @param output
 */
def execute(parsedQuery: QueryModel, output: OutputModel): Unit = {
    
    // getting group by cols in string
    var colsGrpBy = ""
		var flag = 0
		for (opr <- parsedQuery.getGroupbyColumns().asScala) {
			if (flag == 0) {
				colsGrpBy = colsGrpBy + opr
				flag = 1
			} else {
				colsGrpBy = colsGrpBy + ", " + opr
			}
		}

    // creating spark session
    val spark: SparkSession = SparkSession.builder()
      .appName("GroupBy Query")
      .master("local[1]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    
    // loading csv file in data frame
    var table_df = spark.read.format("csv").options(Map("header" -> "false", "delimiter" -> "^", "inferSchema" -> "true"))
      .load(GlobalVariables.dataPath + parsedQuery.getTable() + ".csv")

    // printing schema and top 20 rows
    table_df.printSchema()
    table_df.show()

    // convert group by cols to _c{number} etc
    for (i <- 0 until parsedQuery.getGroupbyColumns.size()) {
      parsedQuery.getGroupbyColumns.set(i, "_c" + DataExtractor.getColumnIndex(parsedQuery.getTable(), parsedQuery.getGroupbyColumns().get(i)))
    }
    // convert it to scala sequence
    var grpByColSeq = asScalaBuffer(parsedQuery.getGroupbyColumns()).toSeq

    // get where comparator col in terms of _c{number}
    var whereCompCol: String = "_c" + DataExtractor.getColumnIndex(parsedQuery.getTable(), parsedQuery.getWhereComparator())

    // get where comparision value
    var whereComp: String = parsedQuery.getWhereComparision()

    // agg fun
    var aggFunc: String = parsedQuery.getAggregateFunction()

    // agg col -- in terms of _c{number} (can be * in case of count)
    var aggCol: String = parsedQuery.getComparisonColumn()
    if (parsedQuery.getComparisonColumn() != "*")
      aggCol = "_c" + DataExtractor.getColumnIndex(parsedQuery.getTable(), parsedQuery.getComparisonColumn())

    // comparision type
    var compType: String = parsedQuery.getComparisonType()

    //val comparision number
    var compNo: Double = parsedQuery.getComparisonNumber()

    // var to store result with where filter applied
    var res = table_df
    
    // time starts
    val startTime = Time.now()
    
    // WHERE clause filter
    if (isNumeric(whereComp))
      res = table_df.filter(whereCompCol + " == " + whereComp)
    else
      res = table_df.filter(whereCompCol + " == '" + whereComp + "'")


    if (compType == ">=") {
      if (aggFunc.equalsIgnoreCase("COUNT")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .count().as("count")
          .filter("count >= " + compNo);
      } else if (aggFunc.equalsIgnoreCase("SUM")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(sum(aggCol).as("sum"))
          .filter("sum >= " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MAX")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(max(aggCol).as("max"))
          .filter("max >= " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MIN")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(min(aggCol).as("min"))
          .filter("min >= " + compNo);
      }
    } else if (compType == ">") {
      if (aggFunc.equalsIgnoreCase("COUNT")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .count().as("count")
          .filter("count > " + compNo);
      } else if (aggFunc.equalsIgnoreCase("SUM")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(sum(aggCol).as("sum"))
          .filter("sum > " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MAX")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(max(aggCol).as("max"))
          .filter("max > " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MIN")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(min(aggCol).as("min"))
          .filter("min > " + compNo);
      }
    } else if (compType == "<=") {
      if (aggFunc.equalsIgnoreCase("COUNT")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .count().as("count")
          .filter("count <= " + compNo);
      } else if (aggFunc.equalsIgnoreCase("SUM")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(sum(aggCol).as("sum"))
          .filter("sum <= " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MAX")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(max(aggCol).as("max"))
          .filter("max <= " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MIN")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(min(aggCol).as("min"))
          .filter("min <= " + compNo);
      }
    } else if (compType == "<") {
      if (aggFunc.equalsIgnoreCase("COUNT")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .count().as("count")
          .filter("count < " + compNo);
      } else if (aggFunc.equalsIgnoreCase("SUM")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(sum(aggCol).as("sum"))
          .filter("sum < " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MAX")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(max(aggCol).as("max"))
          .filter("max < " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MIN")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(min(aggCol).as("min"))
          .filter("min < " + compNo);
      }
    } else if (compType == "=") {
      if (aggFunc.equalsIgnoreCase("COUNT")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .count().as("count")
          .filter("count == " + compNo);
      } else if (aggFunc.equalsIgnoreCase("SUM")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(sum(aggCol).as("sum"))
          .filter("sum == " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MAX")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(max(aggCol).as("max"))
          .filter("max == " + compNo);
      } else if (aggFunc.equalsIgnoreCase("MIN")) {
        res = res.groupBy(grpByColSeq.head, grpByColSeq.tail: _*)
          .agg(min(aggCol).as("min"))
          .filter("min == " + compNo);
      }
    }

    res.show()
    
    //time end
    val endTime = Time.now()
    
    //write time to output model
    output.setSparkExecutionTime(String.valueOf(endTime - startTime) + " milliseconds")
    
    // write result to output csv file
    val outputPathString = GlobalVariables.sparkOutputPath
		res.repartition(1).write.mode(SaveMode.Overwrite).csv(outputPathString)
		
		// writing spark transformation
		var sparkTransformations = parsedQuery.getTable() + ".filter(" + parsedQuery.getWhereComparator() + " == " + parsedQuery.getWhereComparision() + ")"
		sparkTransformations = sparkTransformations + ".groupBy(" + colsGrpBy + ")"
		sparkTransformations = sparkTransformations + ".agg(" + parsedQuery.getAggregateFunction() + "(" + parsedQuery.getComparisonColumn() + "))"
		sparkTransformations = sparkTransformations + ".filter(" + parsedQuery.getSelectColumns().get(parsedQuery.getSelectColumns().size()-1) + " " + parsedQuery.getComparisonType() + " " + parsedQuery.getComparisonNumber() + ")"
		output.setSparkTransformations(sparkTransformations)
		
		val queryres = res.toJSON.collect().toList
		output.setQueryResult(queryres.toString())
  }

  // method to check if string is numeric or not
  def isNumeric(str: String): Boolean = str.matches("[-+]?\\d+(\\.\\d+)?")

  // Test method
  def testScala(a: Int, b: Int): Int = a * b

  // Main Method
  def main(args: Array[String]) {
    // prints Hello World
    println("Hello World!")
  }

}
package org.apache.zeppelin.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.commons.io.IOUtils
import java.net.URL
import java.nio.charset.Charset

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession

// https://spark.apache.org/docs/2.0.2/

object SparkUtils {

  val importStatements: String =
    "import org.joda.time.DateTime\n" +
    "import com.sumologic.notebook.client.{SumoClient, SumoQuery, SumoApiConfig}\n" +
    "import com.sumologic.client.model.LogMessage\n" +
    "import org.apache.spark.rdd.RDD\n" +
    "import org.apache.spark.sql.types._\n" +
    "import org.apache.spark.sql._\n"

  def createSumoClientStr(accessid: String, accesskey: String): String =
    "val sumoClient = new SumoClient(SumoApiConfig(\"" +
    s"$accessid" + "\", \"" +
    s"$accesskey" + "\"))"

  def runQueryStr(query: String,
               startMs: Long,
               endMs: Long) : String =
    s"val queryJob = sumoClient.runQuery(SumoQuery($startMs, $endMs, $query))"
//  "val messages = sumoClient.retrieveAllMessages(100)(queryJob)

  def registerMessagesToRDDStr(viewName: String): String =
    "def messagesToRDD(messages: IndexedSeq[LogMessage]): DataFrame = {" +
    "  import scala.collection.JavaConversions._" +
    "  val fieldsList = asScalaSet(messages(0).getFieldNames).toList" +
    "  val fields = fieldsList.map(fieldName => StructField(fieldName, StringType, nullable = true))" +
    "  val schema = StructType(fields)" +
    "  // Convert records of the RDD (people) to Rows" +
    "  val rowRDD = messages.map(message => Row.fromSeq(fieldsList.map(message.stringField)))" +
    "  // Apply the schema to the RDD" +
    "  val messagesDF = spark.createDataFrame(rowRDD, schema)" +
    "  // Creates a temporary view using the DataFrame" +
    "  messagesDF.createOrReplaceTempView(\"" + s"$viewName" + "\")" +
    "  messagesDF" +
    "}"



  def createRdd[T](data: Seq[T], sc: SparkContext)(implicit c: ClassTag[T]) : RDD[T] = {
    val distData = sc.parallelize(data)
    distData
  }

  def createRddFromJson(sqlContext: SQLContext) : DataFrame = {
    val fileName = "/Users/armin/Source/people.json"
    val json = sqlContext.read.json(fileName)
    json.createOrReplaceGlobalTempView("people")
    json
  }

  def simpleRdd(sc: SparkContext) : RDD[Row] = {
      val rowsRdd: RDD[Row] = sc.parallelize(
        Seq(
          Row("first", 2.0, 7.0),
          Row("second", 3.5, 2.5),
          Row("third", 7.0, 5.9)
        )
      )
    rowsRdd
  }

  def simpleSchema() : StructType = {
    val schema = new StructType()
        .add(StructField("id", StringType, true))
        .add(StructField("val1", DoubleType, true))
        .add(StructField("val2", DoubleType, true))
    schema
  }

  def simpleRdd2(sc: SparkContext, sqlc: SQLContext) : Unit = {
    val rowsRdd: RDD[Row] = sc.parallelize(
      Seq(
        Row("first", 2.0, 7.0),
        Row("second", 3.5, 2.5),
        Row("third", 7.0, 5.9)
      )
    )
    val schema = new StructType()
        .add(StructField("id", StringType, true))
        .add(StructField("val1", DoubleType, true))
        .add(StructField("val2", DoubleType, true))

    val df = sqlc.createDataFrame(rowsRdd, schema)
    df.createOrReplaceGlobalTempView("simple")
  }

//  def bankExample(sc: SparkContext, sqlc: SQLContext) : Unit = {
//
//    val bankText = sc.parallelize(
//      IOUtils.toString(
//        new URL("https://s3.amazonaws.com/apache-zeppelin/tutorial/bank/bank.csv"),
//        Charset.forName("utf8")).split("\n"))
//
//    case class Bank(age: Integer,
//                    job: String,
//                    marital: String,
//                    education: String,
//                    balance: Integer)
//
//    val bankRdd = bankText.map(s => s.split(";")).filter(s => s(0) != "\"age\"").map(
//      s => Bank(s(0).toInt,
//        s(1).replaceAll("\"", ""),
//        s(2).replaceAll("\"", ""),
//        s(3).replaceAll("\"", ""),
//        s(5).replaceAll("\"", "").toInt))
//
//    //import sqlc.implicits._
//    //val bank = bankRdd.toDF()
//    val bank = sqlc.createDataFrame(bankRdd)
//
//    bank.createOrReplaceGlobalTempView("bank2")
//  }


//  def messagesToRDD(messages: IndexedSeq[String],
//                    viewName: String = "messages",
//                    sqlc: SQLContext): DataFrame = {
//    import sqlc.implicits._
//    val messages1 = IndexedSeq("World")
//    val fieldsList = List("Hello")
//    val fields = fieldsList.map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
//    // Convert records of the RDD (people) to Rows
//    val rowRDD = messages1.map(message => Row.fromSeq(fieldsList.map(message)))
//    // Apply the schema to the RDD
//    val messagesDF = sqlc.createDataFrame(rowRDD, schema)
//    // Creates a temporary view using the DataFrame
//    messagesDF.createOrReplaceTempView(s"$viewName")
//    messagesDF
//  }

}

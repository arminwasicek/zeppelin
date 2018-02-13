package org.apache.zeppelin.spark.utils

import java.security.MessageDigest
import java.util.concurrent.ThreadLocalRandom

import com.sumologic.client.metrics.model.CreateMetricsJobResponse
import com.sumologic.notebook.client.{QueryJob, SumoClient, SumoMetricsClient, SumoQuery}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, max, min}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.types._
import com.sumologic.client.model.LogMessage
import vegas.Vegas
import vegas.spec.Spec.MarkEnums.Bar
import vegas.spec.Spec.TypeEnums.{Ordinal, Quantitative}
import collection.JavaConversions._

import collection.JavaConversions._

object SparkSumoUtils {
  private val tempTableBaseName = "myquery"
  var metricsClient : Option[SumoMetricsClient] = None
  var sumoClient : Option[SumoClient] = None
  var result : Option[Exception] = None

  val importStatements: String =
    "import org.joda.time.DateTime\n" +
        "import com.sumologic.notebook.client.{SumoClient, SumoMetricsClient, SumoQuery, SumoApiConfig}\n" +
        "import com.sumologic.client.model.LogMessage\n" +
        "import org.apache.spark.rdd.RDD\n" +
        "import org.apache.spark.sql.types._\n" +
        "import org.apache.spark.sql._\n" +
        "import vegas._\n" +
        "import vegas.data.External._\n" +
        "import org.apache.zeppelin.spark.utils.SparkSumoUtils\n" +
        "import collection.JavaConversions._\n" +
        "import java.security.MessageDigest\n" +
        "import com.sumologic.client.metrics.model.CreateMetricsJobResponse\n"

  def createSumoClientStr(accessid: String, accesskey: String, endpoint: String): String =
    "SparkSumoUtils.sumoClient = Some(new SumoClient(SumoApiConfig(\"" +
        s"$accessid" + "\", \"" +
        s"$accesskey" + "\", \"" +
        s"$endpoint" + "\")))"


  def createSumoMetricsClientStr(accessid: String, accesskey: String, endpoint: String): String =
    "SparkSumoUtils.metricsClient = Some(new SumoMetricsClient(SumoApiConfig(\"" +
        s"$accessid" + "\", \"" +
        s"$accesskey" + "\", \"" +
        s"$endpoint" + "\")))"


  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }

  def runQueryStr(query: String,
                  startMs: Long,
                  endMs: Long): String =
    "val queryJob = SparkSumoUtils.sumoClient.get.runQuery(" +
        s"""SumoQuery(${startMs.toString}L, ${endMs.toString}L, """ +
        s"""${escape(query)}))""" + "\n"

  def runMetricsQueryStr(query: String,
                  startMs: Long,
                  endMs: Long): String =

    "val metricsResponse = SparkSumoUtils.metricsClient.get.runQuery(" +
        s"""SumoQuery(${startMs.toString}L, ${endMs.toString}L, """ +
        s"""${escape(query)}))""" + "\n"


  def runMetricsQuery(query: String,
                      startMs: Long,
                      endMs: Long,
                      viewName: String = "mymetrics")
                      (implicit spark: SparkSession): DataFrame = {
    SparkSumoUtils.result = None
    val response : Option[CreateMetricsJobResponse] = try {
      Some(metricsClient.get.runQuery(SumoQuery(startMs, endMs, query)))
    } catch {
      case e: Exception => {
        SparkSumoUtils.result = Some(e)
        None
      }
    }
    metricsToInstantsDF(response.get, viewName)(spark)
  }

  def runLogQuery(query: String,
                      startMs: Long,
                      endMs: Long): QueryJob = {
    sumoClient.get.runQuery(SumoQuery(startMs, endMs, query))
  }

  def createTempTableName : String = {
    tempTableBaseName + ThreadLocalRandom.current.nextInt(100, 1000)
  }


  def messagesToDF(messages: IndexedSeq[LogMessage],
                     viewName: String)
                    (implicit spark: SparkSession): DataFrame = {
    import scala.collection.JavaConversions._
    if (messages == Vector()) {
      val schema_rdd = StructType("".split(",").map(fieldName =>
        StructField(fieldName, StringType, true)))
      val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema_rdd)
      emptyDF.createOrReplaceTempView(viewName)
      emptyDF
    }
    else {
      val fieldsList = asScalaSet(messages(0).getFieldNames).toList
      val fields = fieldsList.map(fieldName => StructField(fieldName,
        StringType, nullable = true))
      val schema = StructType(fields)
      // Convert records of the RDD (people) to Rows
      val rowRDD = messages.map(message => Row.fromSeq(fieldsList.map(message.stringField)))
      // Apply the schema to the RDD
      val messagesDF = spark.createDataFrame(rowRDD, schema)
      // Creates a temporary view using the DataFrame
      messagesDF.createOrReplaceTempView(viewName)
      messagesDF
    }
  }

  def metricsToInstantsDF(metrics: CreateMetricsJobResponse, viewName: String)
                   (implicit spark: SparkSession): DataFrame = {
    def hash(s: String) = MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString("")
    val fieldsList = metrics.map(metric => {
      hash(metric.getDimensions).take(6)
    })
    val fields = fieldsList.map(fieldName => StructField(fieldName, DoubleType, nullable = true))
    val dateField = Seq(StructField("timestamp", LongType, nullable = true))
    val schema = StructType(dateField ++ fields.toSeq)
    val timestamps = metrics.toSeq.head.getTimestamps
    val rows = metrics.map(_.getTimestamps.toSeq.length).min
    val rowRDD = (0 until rows).map(idx =>
      Row.fromSeq(Seq(timestamps(idx).getMillis) ++ metrics.map(m => m.getValues.toList(idx)).toSeq))
    val instantsDF = spark.createDataFrame(rowRDD.toList, schema)
    instantsDF.createOrReplaceTempView(viewName)
    instantsDF
  }

  def metricsToObservationDF(metricsResponse: CreateMetricsJobResponse, viewName: String)
                             (implicit spark: SparkSession): DataFrame = {
    def hash(s: String) = MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString("")

    val obsSchema = StructType(Seq(StructField("timestamp", LongType, nullable = true),
      StructField("key", StringType, nullable = true),
      StructField("value", DoubleType, nullable = true)))

    val obsRDD = metricsResponse.flatMap(m => {
      val key = hash(m.getDimensions).take(6)
      val ts = m.getTimestamps.toList
      val v = m.getValues.toList
      (ts zip v).map(_ match {case (ts, v) =>
        Row.fromSeq(Seq(ts.getMillis, key, v).toSeq)
      })
    }).toList
    val obsDF = spark.createDataFrame(obsRDD, obsSchema)
    obsDF.createOrReplaceTempView(viewName)
    obsDF
  }


  def computeHistogram(myquery: DataFrame, col: String = "_messagetime"): Unit = {
    val myqueryi = myquery.selectExpr("cast(" + col + " as double) " + col)
    val Row(minValue: Double, maxValue: Double) = myqueryi.agg(min(col), max(col)).head

    val buckets = 9
    val splits = (0 to buckets).map(v => minValue + (v.toDouble * (maxValue - minValue) / buckets)).toArray

    val bucketizer = new Bucketizer()
        .setInputCol(col)
        .setOutputCol("bucket")
        .setSplits(splits)

    val bucketed: DataFrame = bucketizer.transform(myqueryi)
    val histo = bucketed.groupBy("bucket").agg(count(col).as("count"))

    val chart = histo.collect().map((r: Row) => Map("Time" -> r(0), "Count" -> r(1))).toList
    Vegas("Histogram view.", width=Some(600.0), height=Some(100.0)).
        withData(chart).encodeX("Time", Ordinal).encodeY("Count", Quantitative).mark(Bar).show
  }

}

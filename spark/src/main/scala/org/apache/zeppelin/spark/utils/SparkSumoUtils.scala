package org.apache.zeppelin.spark.utils

import java.security.MessageDigest

import com.sumologic.client.metrics.model.CreateMetricsJobResponse
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{count, max, min}
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import com.sumologic.client.model.LogMessage
import vegas.Vegas
import vegas.spec.Spec.MarkEnums.Bar
import vegas.spec.Spec.TypeEnums.{Ordinal, Quantitative}

import collection.JavaConversions._

object SparkSumoUtils {

  val importStatements: String =
    "import org.joda.time.DateTime\n" +
        "import com.sumologic.notebook.client.{SumoClient, SumoQuery, SumoApiConfig}\n" +
        "import com.sumologic.client.model.LogMessage\n" +
        "import org.apache.spark.rdd.RDD\n" +
        "import org.apache.spark.sql.types._\n" +
        "import org.apache.spark.sql._\n" +
        "import vegas._\n" +
        "import vegas.data.External._\n" +
        "import org.apache.zeppelin.spark.utils.SparkSumoUtils\n" +
        "import collection.JavaConversions._\n"

  def createSumoClientStr(accessid: String, accesskey: String, endpoint: String): String =
    "val sumoClient = new SumoClient(SumoApiConfig(\"" +
        s"$accessid" + "\", \"" +
        s"$accesskey" + "\", \"" +
        s"$endpoint" + "\"))"

  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }

  def runQueryStr(query: String,
                  startMs: Long,
                  endMs: Long): String =
    "val queryJob = sumoClient.runQuery(" +
        s"""SumoQuery(${startMs.toString}L, ${endMs.toString}L, """ +
        s"""${escape(query)}))""" + "\n"

  def registerMessagesToRDDStr(viewName: String): String =
    "def messagesToRDD(messages: IndexedSeq[LogMessage]): DataFrame = {\n" +
        "  import scala.collection.JavaConversions._\n" +
        "  if (messages == Vector()) { \n" +
        "    val schema_rdd = StructType(\"\".split(\",\").map(fieldName => StructField(fieldName, StringType, true)) )\n" +
        "    val emptyDF = spark.createDataFrame(sc.emptyRDD[Row], schema_rdd)\n" +
        s"""    emptyDF.createOrReplaceTempView("$viewName")""" + "\n" +
        "    emptyDF\n" +
        "  }\n" +
        "  else {\n" +
        "    val fieldsList = asScalaSet(messages(0).getFieldNames).toList\n" +
        "    val fields = fieldsList.map(fieldName => StructField(fieldName,\n" +
        "      StringType, nullable = true))\n" +
        "    val schema = StructType(fields)\n" +
        "    // Convert records of the RDD (people) to Rows\n" +
        "    val rowRDD = messages.map(message => Row.fromSeq(fieldsList.map(message.stringField)))\n" +
        "    // Apply the schema to the RDD\n" +
        "    val messagesDF = spark.createDataFrame(rowRDD, schema)\n" +
        "    // Creates a temporary view using the DataFrame\n" +
        s"""    messagesDF.createOrReplaceTempView("$viewName")""" + "\n" +
        "    messagesDF\n" +
        "  }\n" +
        "}"

  def messagesToRDD2(messages: IndexedSeq[LogMessage],
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

  def metricsToRDD2(metrics: CreateMetricsJobResponse)
                   (implicit spark: SparkSession): DataFrame = {
    def getSchema(metricsResponse: CreateMetricsJobResponse): StructType = {
      def hash(s: String) = MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8")).map("%02x".format(_)).mkString("")

      val fieldsList = metricsResponse.map(metric => {
        hash(metric.getDimensions).take(6)
      })
      val fields = fieldsList.map(fieldName =>
        StructField(fieldName, DoubleType, nullable = true))
      StructType(fields.toSeq)
    }

    val schema = getSchema(metrics)
    val rowRDD = (0 until metrics.toSeq.length).map(idx =>
      Row.fromSeq(metrics.map(m => m.getValues().toList(idx)).toSeq))
    val metricsDF = spark.createDataFrame(rowRDD.toList, schema)
    metricsDF.createOrReplaceTempView("mymetric")
    metricsDF
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

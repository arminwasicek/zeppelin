package org.apache.zeppelin.spark.utils

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

  def escape(raw: String): String = {
    import scala.reflect.runtime.universe._
    Literal(Constant(raw)).toString
  }

  def runQueryStr(query: String,
               startMs: Long,
               endMs: Long) : String =
    "val queryJob = sumoClient.runQuery(" +
      s"""SumoQuery(${startMs.toString}L, ${endMs.toString}L, """ +
      s"""${escape(query)}))"""


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

}

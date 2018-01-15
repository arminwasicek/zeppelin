package org.apache.zeppelin.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.spark.util.ParseDate;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.nsc.interpreter.Results;

import java.lang.reflect.Field;
import java.util.*;


/**
 *
 */
public class SparkSumoInterpreter extends SparkSqlInterpreter {
  Logger logger = LoggerFactory.getLogger(SparkSumoInterpreter.class);
  private SparkILoop interpreter = null;
  private Object intp = null;

  class QueryTriplet {
    String query;
    DateTime startQuery;
    DateTime endQuery;

    public QueryTriplet(String query, DateTime startQuery, DateTime endQuery) {
      this.query = query;
      this.startQuery = startQuery;
      this.endQuery = endQuery;
    }
  }

  public SparkSumoInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    super.open();
    try {
      SparkInterpreter sparkInterpreter = getSparkInterpreter();
      Field f = sparkInterpreter.getClass().getDeclaredField("interpreter");
      f.setAccessible(true);
      interpreter = (SparkILoop) f.get(sparkInterpreter);
      intp = Utils.invokeMethod(interpreter, "intp");

    } catch (NoSuchFieldException nfse) {
      throw new InterpreterException(nfse);
    } catch (IllegalAccessException iae) {
      throw new InterpreterException(iae);
    }
  }

  private String getJobGroup(InterpreterContext context){
    return "zeppelin-" + context.getParagraphId();
  }

  private Results.Result interpret(String line) {
    return (Results.Result) Utils.invokeMethod(
            intp,
            "interpret",
            new Class[] {String.class},
            new Object[] {line});
  }

  @Override
  public InterpreterResult interpret(String paragraph, InterpreterContext context) {
    logger.error(">>>> SUMO INTERPRET");
    QueryTriplet triplet = parseQueryTripletFromParagraph(paragraph);

    // Logging query triplet
    logger.info("Query: " + triplet.query);
    logger.info("QueryStart: " + triplet.startQuery);
    logger.info("QueryEnd  : " + triplet.endQuery);
    logger.info("Accesskey : " + getProperty("zeppelin.spark.sumoAccesskey"));
    logger.info("Accessid  : " + getProperty("zeppelin.spark.sumoAccessid"));

    //z.input("name")

    SQLContext sqlc = null;
    SparkInterpreter sparkInterpreter = getSparkInterpreter();

    if (sparkInterpreter.getSparkVersion().isUnsupportedVersion()) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, "Spark "
              + sparkInterpreter.getSparkVersion().toString() + " is not supported");
    }

    sparkInterpreter.populateSparkWebUrl(context);
    sqlc = getSparkInterpreter().getSQLContext();
    SparkContext sc = sqlc.sparkContext();

    //sparkInterpreter.

    //Object intp = Utils.invokeMethod(sparkInterpreter, "intp");



    interpret("val a = 1");

    String importStatements =
      "import org.apache.spark.rdd.RDD\n" +
      "import org.apache.spark.sql.types._\n" +
      "import org.apache.spark.sql._\n";

    String instantiateRdd =
      "val rowsRdd: RDD[Row] = sc.parallelize(\n" +
      "        Seq(\n" +
      "                Row(\"first\", 2.0, 7.0),\n" +
      "                Row(\"second\", 3.5, 2.5),\n" +
      "                Row(\"third\", 7.0, 5.9)\n" +
      "        )\n" +
      ")\n";

    String instantiateSchema =
      "val schema = new StructType()\n" +
      "        .add(StructField(\"id\", StringType, true))\n" +
      "        .add(StructField(\"val1\", DoubleType, true))\n" +
      "        .add(StructField(\"val2\", DoubleType, true))\n";

    String createDsView =
      "val df = spark.createDataFrame(rowsRdd, schema)\n" +
      "df.createOrReplaceTempView(\"simple\")\n";

    sparkInterpreter.interpret(
      importStatements + instantiateRdd + instantiateSchema + createDsView, context);

//    sc.setJobGroup(getJobGroup(context), "Zeppelin", false);
//
//    JavaRDD<Row> rdd = SparkUtils.simpleRdd(sc).toJavaRDD();
//    StructType schema = SparkUtils.simpleSchema();
//    //Dataset<Row> df = sqlc.createDataFrame(rdd, schema);
//    //df.createOrReplaceGlobalTempView("simple");
//
//
//    Object df = null;
//    try {
//      Method createDataFrameMethod = sqlc.getClass().getMethod("createDataFrame",
//        JavaRDD.class, StructType.class);
//      df = createDataFrameMethod.invoke(sqlc, rdd, schema);
//      Method createOrReplaceGlobalTempView =
//        df.getClass().getMethod("createOrReplaceGlobalTempView", String.class);
//      createOrReplaceGlobalTempView.invoke(df, "simple");
//    } catch (Exception e) {
//      throw new InterpreterException(e);
//    }



    //SparkUtils.simpleRdd(sc, sqlc);


    //SparkUtils.createRddFromJson(sqlc);

//    OK
//    sparkInterpreter.interpret("val data = Array(1, 2, 3, 4, 5)\n" +
//      "val distData = sc.parallelize(data)", context);

//    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//    SparkUtils.createRdd(JavaConverters.asScalaIterableConverter(data).asScala().toSeq(), sc);


    String sqlQuery = "select age, count(1) value\n" +
            "from bank \n" +
            "where age < 30 \n" +
            "group by age \n" +
            "order by age";
    return super.interpret(sqlQuery, context);
  }

  private SparkInterpreter getSparkInterpreter() {
    LazyOpenInterpreter lazy = null;
    SparkInterpreter spark = null;
    Interpreter p = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class.getName());

    while (p instanceof WrappedInterpreter) {
      if (p instanceof LazyOpenInterpreter) {
        lazy = (LazyOpenInterpreter) p;
      }
      p = ((WrappedInterpreter) p).getInnerInterpreter();
    }
    spark = (SparkInterpreter) p;

    if (lazy != null) {
      lazy.open();
    }
    return spark;
  }

  enum ParserState {QUERYORDATE, QUERYPARSE, DATEPARSE}

  private void addLineToQuery(String line, StringBuffer query) {
    if (line.trim().length() > 0) {
      query.append(line + "\n");
    }
  }

  private QueryTriplet parseQueryTripletFromParagraph(String paragraph) {
    // State machine to parse paragraph
    //    --  https://www.mirkosertic.de/blog/2013/04/implementing-state-machines-with-java-enums/
    StringBuffer query = new StringBuffer();
    DateTime queryEnd = DateTime.now();
    DateTime queryStart = queryEnd.minusMinutes(15);

    String lines[] = paragraph.split("\n");
    List<DateTime> parsedDates = new ArrayList<>();
    ParserState state = ParserState.QUERYORDATE;
    for (String paragraphLine: lines) {
      DateTime res = ParseDate.parse(paragraphLine.trim());
      switch (state) {
          case QUERYORDATE:
            if (res == null) {
              state = ParserState.QUERYPARSE;
              addLineToQuery(paragraphLine, query);
            }
            else {
              parsedDates.add(res);
            }
            break;
          case QUERYPARSE:
            if (res == null) {
              addLineToQuery(paragraphLine, query);
            }
            else {
              state = ParserState.DATEPARSE;
              parsedDates.add(res);
            }
            break;
          case DATEPARSE:
            if (res != null) {
              parsedDates.add(res);
            }
            break;
      }
    }
    Collections.sort(parsedDates);
    if (parsedDates.size() >= 2) {
      queryStart = parsedDates.get(0);
      queryEnd = parsedDates.get(parsedDates.size() - 1);
    }
    return new QueryTriplet(query.toString(), queryStart, queryEnd);
  }
}

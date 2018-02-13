package org.apache.zeppelin.spark;

import com.sumologic.notebook.client.QueryJob;
import org.apache.spark.SparkContext;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.spark.util.ParseDate;
import org.apache.zeppelin.spark.utils.SparkSumoUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.None;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.interpreter.Results;
import scala.collection.JavaConverters;
import java.lang.reflect.Field;
import java.util.*;

/**
 *
 */
public class SparkSumoInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(SparkSumoInterpreter.class);
  private SparkILoop interpreter = null;
  private Object intp = null;
  private int maxResult;
  private int progress = 0;


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
  public void close() {}

  @Override
  public int getProgress(InterpreterContext context) {
    //SparkInterpreter sparkInterpreter = getSparkInterpreter();
    //return sparkInterpreter.getProgress(context);
    return progress;
  }

  private String getJobGroup(InterpreterContext context){
    return "zeppelin-" + context.getParagraphId();
  }

  @Override
  public void cancel(InterpreterContext context) {
    SQLContext sqlc = getSparkInterpreter().getSQLContext();
    SparkContext sc = sqlc.sparkContext();

    sc.cancelJobGroup(getJobGroup(context));
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public void open() {
    this.maxResult = Integer.parseInt(getProperty("zeppelin.spark.maxResult"));
    try {
      SparkInterpreter sparkInterpreter = getSparkInterpreter();
      Field f = sparkInterpreter.getClass().getDeclaredField("interpreter");
      f.setAccessible(true);
      interpreter = (SparkILoop) f.get(sparkInterpreter);
      intp = Utils.invokeMethod(interpreter, "intp");
      interpret(SparkSumoUtils.importStatements());
      String accessKey = getProperty("zeppelin.spark.sumoAccesskey");
      String accessId  = getProperty("zeppelin.spark.sumoAccessid");
      String endpoint  = getProperty("zeppelin.spark.sumoEndpoint");
      interpret(SparkSumoUtils.createSumoClientStr(accessId, accessKey, endpoint));
      interpret(SparkSumoUtils.createSumoMetricsClientStr(accessId, accessKey, endpoint));
      interpret("implicit val render = vegas.render.ShowHTML(s => print(\"%html \" + s))");
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new InterpreterException(e);
    }
  }

  private InterpreterResult.Code getResultCode(scala.tools.nsc.interpreter.Results.Result r) {
    if (r instanceof scala.tools.nsc.interpreter.Results.Success$) {
      return InterpreterResult.Code.SUCCESS;
    } else if (r instanceof scala.tools.nsc.interpreter.Results.Incomplete$) {
      return InterpreterResult.Code.INCOMPLETE;
    } else {
      return InterpreterResult.Code.ERROR;
    }
  }

  public Object getValue(String name) {
    Object ret = Utils.invokeMethod(
            intp, "valueOfTerm", new Class[]{String.class}, new Object[]{name});
    if (ret instanceof None) {
      return null;
    } else if (ret instanceof Some) {
      return ((Some) ret).get();
    } else {
      return ret;
    }
  }

  public Object getLastObject() {
    IMain.Request r = (IMain.Request) Utils.invokeMethod(intp, "lastRequest");
    Object obj = r.lineRep().call("$result",
            JavaConversions.asScalaBuffer(new LinkedList<>()));
    return obj;
  }


  private Results.Result interpret(String line) {
    Results.Result res = (Results.Result) Utils.invokeMethod(
            intp,
            "interpret",
            new Class[] {String.class},
            new Object[] {line});
    InterpreterResult.Code r = getResultCode(res);
    return res;
  }

  @Override
  public InterpreterResult interpret(String paragraph, InterpreterContext context) {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    ZeppelinContext z = sparkInterpreter.getZeppelinContext();
    z.setInterpreterContext(context);
    z.setGui(context.getGui());

    logger.error(">>>> SUMO INTERPRET <<<<");
//    String tempTableName = tempTableBaseName +
//            ThreadLocalRandom.current().nextInt(100, 1000);
    String tempTableName = SparkSumoUtils.createTempTableName();
    String resultName = (String) z.input("Dataframe", tempTableName);

    QueryTriplet triplet = parseQueryTripletFromParagraph(paragraph);

    progress += 10;
    // Logging query triplet
    logger.info("Query: " + triplet.query);
    logger.info("QueryStart  : " + triplet.startQuery);
    logger.info("QueryEnd    : " + triplet.endQuery);

    // Display textfields
    ArrayList<Tuple2<Object, String>> options = new ArrayList<>();
    options.add(new Tuple2<>((Object) "log", "Log"));
    options.add(new Tuple2<>((Object) "metrics", "Metrics"));
    Seq<Tuple2<Object, String>> optionsSeq =
            JavaConverters.asScalaBufferConverter(options).asScala().toSeq();
    String queryType = z.select("Query type", optionsSeq).toString();
    if (queryType == "") {
      queryType = z.select("Query type", (Object) "log", optionsSeq).toString();
    }

    String startQueryStr = triplet.startQuery.toString();
    String endQueryStr = triplet.endQuery.toString();
    DateTime startQuery = DateTime.parse((String) z.input("Start Time", startQueryStr));
    DateTime endQuery = DateTime.parse((String) z.input("End Time", endQueryStr));

    logger.info("x QueryStart: " + startQuery);
    logger.info("x QueryEnd  : " + endQuery);
    logger.info("ResName     : " + resultName);
    logger.info("queryType   : " + queryType);


    // Run query
    if ((queryType.equals("")) || (queryType.equals("log"))) {
      String runQuery = SparkSumoUtils.runQueryStr(
              triplet.query, startQuery.getMillis(), endQuery.getMillis());
      Results.Result codeRetrieve = interpret(runQuery);
      logger.info(runQuery);
      if (getResultCode(codeRetrieve) != InterpreterResult.Code.SUCCESS) {
        return new InterpreterResult(getResultCode(codeRetrieve),
                "Something went wrong when retrieving logs.");
      }


      progress += 30;

      // Check result
      QueryJob job = (QueryJob) getLastObject();
      if (job.error().nonEmpty()) {
        String errorMsg = "Query failed : " + job.error().get().getErrorMessage();
        logger.info(errorMsg);
        return new InterpreterResult(InterpreterResult.Code.ERROR, errorMsg);
      } else {
        logger.info("Query successful : exception field empty");
      }
      progress += 10;

      // Convert to RDD
      String convertToRDD = "val " + resultName + " = " +
        "SparkSumoUtils.messagesToDF(" +
              "SparkSumoUtils.sumoClient.get.retrieveAllMessages(100)(queryJob), " +
              "\"" + resultName + "\")(spark)";
      Results.Result codeCovert = interpret(convertToRDD);
      logger.info(convertToRDD);
      if (getResultCode(codeCovert) != InterpreterResult.Code.SUCCESS) {
        return new InterpreterResult(getResultCode(codeCovert),
                "Something went wrong when converting logs.");
      }

      progress += 20;

      // Display histogram
      sparkInterpreter.interpret("SparkSumoUtils.computeHistogram(" +
              resultName + ")", context);
      logger.info("Plotted vegas");

      progress += 30;

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, "Log query successful.");
    }
    else if (queryType.equals("metrics")) {
      // Run query
      String runQuery = SparkSumoUtils.runMetricsQueryStr(
              triplet.query, startQuery.getMillis(), endQuery.getMillis());
      Results.Result codeRun = interpret(runQuery);
      logger.info(runQuery);
      if (getResultCode(codeRun) != InterpreterResult.Code.SUCCESS) {
        return new InterpreterResult(getResultCode(codeRun),
                "Something went wrong when retrieving metrics.");
      }

      progress += 40;

      // Check result
      
      // Convert to RDD
      String convertToRDD = "val " + resultName + " = " +
              "SparkSumoUtils.metricsToInstantsDF(metricsResponse, " +
              "\"" + resultName + "\")(spark)";
      Results.Result codeRetrieve = interpret(convertToRDD);
      logger.info(convertToRDD);
      if (getResultCode(codeRetrieve) != InterpreterResult.Code.SUCCESS) {
        return new InterpreterResult(getResultCode(codeRetrieve),
                "Something went wrong when converting metrics.");
      }


      progress += 40;

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, "Metrics query successful.");
    }
    return new InterpreterResult(InterpreterResult.Code.ERROR, "Query type invalid.");
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
    ArrayList<DateTime> parsedDates = new ArrayList<>();
    ParserState state = ParserState.QUERYORDATE;
    for (String paragraphLine: lines) {
      DateTime res = ParseDate.parse(paragraphLine.trim());
      logger.info(res == null ? "null" : res.toString());
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
    logger.info("PARSEDATE SIZE" + parsedDates.size());
    Collections.sort(parsedDates);
    logger.info("PARSEDATE SIZE" + parsedDates.size());
    if (parsedDates.size() >= 2) {
      queryStart = parsedDates.get(0);
      queryEnd = parsedDates.get(parsedDates.size() - 1);
    }
    return new QueryTriplet(query.toString(), queryStart, queryEnd);
  }
}

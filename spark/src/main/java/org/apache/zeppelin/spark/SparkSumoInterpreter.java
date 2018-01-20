package org.apache.zeppelin.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.repl.SparkILoop;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.spark.util.ParseDate;
import org.apache.zeppelin.spark.utils.SparkSumoUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.nsc.interpreter.Results;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


/**
 *
 */
public class SparkSumoInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(SparkSumoInterpreter.class);
  private SparkILoop interpreter = null;
  private Object intp = null;
  String tempTableBaseName = "myquery";
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
    String tempTableName = tempTableBaseName +
            ThreadLocalRandom.current().nextInt(100, 1000);
    String resultName = (String) z.input("Dataframe", tempTableName);
    interpret(SparkSumoUtils.registerMessagesToRDDStr(resultName));

    QueryTriplet triplet = parseQueryTripletFromParagraph(paragraph);

    progress += 10;
    // Logging query triplet
    logger.info("Query: " + triplet.query);
    logger.info("QueryStart: " + triplet.startQuery);
    logger.info("QueryEnd  : " + triplet.endQuery);

    // Display textfields
    String startQueryStr = triplet.startQuery.toString();
    String endQueryStr = triplet.endQuery.toString();
    DateTime startQuery = DateTime.parse((String) z.input("Start Time", startQueryStr));
    DateTime endQuery = DateTime.parse((String) z.input("End Time", endQueryStr));
    logger.info("x QueryStart: " + startQuery);
    logger.info("x QueryEnd  : " + endQuery);
    logger.info("ResName  : " + resultName);


    // Run query
    logger.info(">>++ SUMO ++<<" + SparkSumoUtils.runQueryStr(
            triplet.query,
            startQuery.getMillis(),
            endQuery.getMillis()));

    interpret(SparkSumoUtils.runQueryStr(
      triplet.query,
      startQuery.getMillis(),
      endQuery.getMillis()));

    progress += 40;

    interpret("val " + resultName + "= messagesToRDD(" +
      "sumoClient.retrieveAllMessages(100)(queryJob))");


    progress += 20;

    // Display histogram
    sparkInterpreter.interpret("SparkSumoUtils.computeHistogram(" +
      resultName + ")", context);
    logger.info("Plotted vegas");

    progress += 30;

    return new InterpreterResult(InterpreterResult.Code.SUCCESS, "Query successful.");
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

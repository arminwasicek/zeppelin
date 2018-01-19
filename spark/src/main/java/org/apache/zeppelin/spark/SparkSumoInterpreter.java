package org.apache.zeppelin.spark;

import org.apache.spark.repl.SparkILoop;
import org.apache.spark.sql.SQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.spark.util.ParseDate;
import org.apache.zeppelin.spark.utils.SparkUtils;
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
public class SparkSumoInterpreter extends SparkSqlInterpreter {
  Logger logger = LoggerFactory.getLogger(SparkSumoInterpreter.class);
  private SparkILoop interpreter = null;
  private Object intp = null;
  String tempTableBaseName = "myquery";

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
      interpret(SparkUtils.importStatements());
      String accessKey = getProperty("zeppelin.spark.sumoAccesskey");
      String accessId  = getProperty("zeppelin.spark.sumoAccessid");
      interpret(SparkUtils.createSumoClientStr(accessId, accessKey));

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
    interpret(SparkUtils.registerMessagesToRDDStr(resultName));

    QueryTriplet triplet = parseQueryTripletFromParagraph(paragraph);

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

//sparkInterpreter.interpret("z.input(\"Start Time\", \"" + triplet.startQuery + "\")", context);
//    sparkInterpreter.interpret("z.input(\"End Time\", \"" + triplet.endQuery + "\")", context);
//    InterpreterResult resultName =
//      sparkInterpreter.interpret("z.input(\"Dataframe\", \"" + tempTableName + "\")", context);
    logger.info("ResName  : " + resultName);



//    logger.info("DataFrame : " + z.input("Dataframe"));
//    z.setGui(context.getGui());
//    z.input("Text");
//    interpret("z.input(\"StartTime\", \"" + triplet.startQuery + "\")");
//    interpret("z.input(\"EndTime\", \"" + triplet.endQuery + "\")");

    // Run query
    logger.info(">>++ SUMO ++<<" + SparkUtils.runQueryStr(
            triplet.query,
            startQuery.getMillis(),
            endQuery.getMillis()));

    interpret(SparkUtils.runQueryStr(
      triplet.query,
      startQuery.getMillis(),
      endQuery.getMillis()));



    interpret("val " + resultName + "= messagesToRDD(" +
      "sumoClient.retrieveAllMessages(100)(queryJob))");

    SQLContext sqlc = sparkInterpreter.getSQLContext();
    List<String> tables = Arrays.asList(sqlc.tableNames());
    if (!tables.contains(resultName)) {
      return new InterpreterResult(InterpreterResult.Code.ERROR, "Somehow the query went wrong.");
    }

    // Display histogram
    String sqlQuery = "select timestamp, count(*) as messages from  " +
      "(select from_unixtime(_messagetime/1000) as timestamp from " + resultName +
      ") group by timestamp";
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

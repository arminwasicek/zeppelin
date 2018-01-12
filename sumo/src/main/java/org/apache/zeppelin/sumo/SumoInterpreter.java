package org.apache.zeppelin.sumo;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.util.InterpreterOutputStream;
import org.apache.zeppelin.spark.SparkInterpreter;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zeppelin.sumo.util.ParseDate;
import org.apache.zeppelin.interpreter.Interpreter.FormType;

/**
 * Sumo interpreter for Zeppelin.
 *
 */
public class SumoInterpreter extends Interpreter {
  public static Logger logger = LoggerFactory.getLogger(SumoInterpreter.class);
  private InterpreterOutputStream out;
  private int progress = 0;

  public SumoInterpreter(Properties property) {
    super(property);
    out = new InterpreterOutputStream(logger);
  }

  /**
   * Open the interpreter
   */  
  @Override
  public void open() {
    logger.info("Open sumo interpreter");
  }

  /**
   * Optionally implement the canceling routine to abort interpret() method
   */
  @Override
  public void cancel(InterpreterContext context) throws InterpreterException {
    logger.info("Cancelled interpretation");
  }
 
  /**
   * Dynamic form handling
   * see http://zeppelin.apache.org/docs/dynamicform.html
   *
   * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}),
   * FormType.NATIVE handles form in API
   */
  @Override
  public FormType getFormType() throws InterpreterException {
    return FormType.NATIVE;
  }

  enum ParserState {QUERYORDATE, QUERYPARSE, DATEPARSE}

  private void addLineToQuery(String line, StringBuffer query) {
    if (line.trim().length() > 0) {
      query.append(line + "\n");
    }
  }

//  final static int QUERYORDATE = 0;
//  final static int QUERYPARSE = 1;
//  final static int DATEPARSE = 2;

  /**
   * Interpret a single paragraph.
   */
  @Override
  public InterpreterResult interpret(String line, InterpreterContext context) {
    StringBuffer query = new StringBuffer();
    DateTime queryEnd = DateTime.now();
    DateTime queryStart = queryEnd.minusMinutes(15);

    // State machine to parse paragraph
    //    --  https://www.mirkosertic.de/blog/2013/04/implementing-state-machines-with-java-enums/
    String lines[] = line.split("\n");
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
      }
    }
    Collections.sort(parsedDates);
    if (parsedDates.size() >= 2) {
      queryStart = parsedDates.get(0);
      queryEnd = parsedDates.get(parsedDates.size() - 1);
    }

    // Logging query triplet
    logger.info("Query: " + query.toString());
//    List<String> strDates = new ArrayList<>(parsedDates.size());
//    for (DateTime date : parsedDates) {
//      strDates.add(String.valueOf(date));
//    }
    logger.info("QueryStart: " + queryStart);
    logger.info("QueryEnd  : " + queryEnd);

    // Running query

//    for (int i = 0; i < 10; i++) {
//      try {
//        Thread.sleep(200 * i);
//        progress = 10 * i;
//      }
//      catch (InterruptedException e) {
//        logger.info("Sleep has been interrupted");
//      }
//    }

    Random rand = new Random();
    long value = rand.nextLong();

    return new InterpreterResult(Code.SUCCESS,
      InterpreterResult.Type.TEXT,
      "Random result = " + value);
  }

  /**
   * Progress made on interpretation
   *
   * @return number between 0-100
   */
  @Override
  public int getProgress(InterpreterContext context) throws InterpreterException {
    return progress;
  }


  
  /**
   * Close the interpreter
   */
  @Override
  public void close() {
    logger.info("Close sumo interpreter");
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
}


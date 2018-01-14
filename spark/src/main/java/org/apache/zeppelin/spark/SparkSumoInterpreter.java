package org.apache.zeppelin.spark;

import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.spark.util.ParseDate;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class SparkSumoInterpreter extends SparkSqlInterpreter {
  Logger logger = LoggerFactory.getLogger(SparkSumoInterpreter.class);

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
  public InterpreterResult interpret(String paragraph, InterpreterContext context) {
    logger.error(">>>> SUMO INTERPRET");
    QueryTriplet triplet = parseQueryTripletFromParagraph(paragraph);

    // Logging query triplet
    logger.info("Query: " + triplet.query);
    logger.info("QueryStart: " + triplet.startQuery);
    logger.info("QueryEnd  : " + triplet.endQuery);

    String sqlQuery = "select age, count(1) value\n" +
            "from bank \n" +
            "where age < 30 \n" +
            "group by age \n" +
            "order by age";
    return super.interpret(sqlQuery, context);
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

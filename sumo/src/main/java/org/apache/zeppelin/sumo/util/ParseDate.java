package org.apache.zeppelin.sumo.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Parsing date string according to multiple formats
 */
public class ParseDate {

  /**
   * http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html
   */
  static String[] dateFormats = {
    "yyyy-MM-dd",
    "yyyy/MM/dd",
    "yyyy MMM dd",
    "yyyy dd MMM",
    "dd-MM-yyyy",
    "dd/MM/yyyy",
    "dd MMM yyyy",
    "dd MMM yyyy",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd HH:mm z",
    "yyyy-MM-dd HH:mm a",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss z",
    "yyyy-MM-dd HH:mm:ss a",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss.SSSZ",
    "yyyy.MM.dd G 'at' HH:mm:ss z",
    "EEE, d MMM yyyy HH:mm:ss",
    "EEE d, MMM yyyy HH:mm:ss",
    "EEE, d MMM yyyy HH:mm:ss",
    "EEE d, MMM yyyy HH:mm:ss",
    "EEE d, yyyy HH:mm:ss",
    "MMM d YYYY HH:mm:ss",
    "MMM d YYYY HH:mm:ss z",
    "MMM d, yyyy HH:mm:ss",
    "MMM d, yyyy HH:mm:ss z",
    "MMM d, yyyy hh:mm a",
    "MMM d, yyyy hh:mm:ss a",
    "MMM d, yyyy hh:mm:ss a z",
    "EEEEE MMMMM yyyy HH:mm:ss.SSSZ",
    "EEE MMM dd HH:mm:ss z yyyy",
    "YY/M/dd h:mm a",
    "YY/M/dd h:mm:ss a",
    "M/dd h:mm a",
    "M/dd h:mm:ss a",
    "h:mm a",
    "h:mm a z",
    "h:mm:ss a",
    "h:mm:ss a z",
    "H:mm z",
  };

  public static DateTime parse(String source) {
    try {
      //return new DateTime(parse2(source));
      return parse3(source);
    }
    catch (IllegalArgumentException e) {
      //silent
    }
    return null;
  }

  public static Date parse2(String token){
    Locale dutch = new Locale("en", "US");

    for (String formatString : dateFormats) {
      try {
        SimpleDateFormat format = new SimpleDateFormat(formatString, dutch);
        format.setLenient(false);
        Date mydate = format.parse(token);
        return mydate;
      }
      catch (ParseException e) {
                //silent
      }
    }
    return null;
  }

  public static DateTime parse3(String source) {
    DateTimeParser parsers[] = new DateTimeParser[dateFormats.length];
    for (int i = 0; i < dateFormats.length; i++) {
      String pattern = dateFormats[i];
      parsers[i] = DateTimeFormat.forPattern(pattern).getParser();
    }
    DateTimeFormatter formatter =
      new DateTimeFormatterBuilder().append( null, parsers )
        .toFormatter();
    return formatter.parseDateTime(source);
    //return null;
  }
}

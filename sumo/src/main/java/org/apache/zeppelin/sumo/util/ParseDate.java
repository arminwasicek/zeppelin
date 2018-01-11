package org.apache.zeppelin.sumo.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 *
 */
public class ParseDate {
  public static Date parse(String source) {
    DateFormat format =
      DateFormat.getDateTimeInstance(
        DateFormat.MEDIUM, DateFormat.SHORT);

    try {
      return format.parse(source);
    }
    catch (ParseException e) {
      //silent
    }
    return null;
  }

  public static Date parse2(String token){
    String[] date_formats = {
      "yyyy-MM-dd",
      "yyyy/MM/dd",
      "yyyy MMM dd",
      "yyyy dd MMM",
      "dd-MM-yyyy",
      "dd/MM/yyyy",
      "dd MMM yyyy",
      "dd MMM yyyy",
      "yyyy-MM-dd HH:mm:ss",
      "yyyy-MM-dd HH:mm:ss.SSS",
      "yyyy-MM-dd HH:mm:ss.SSSZ",
      "yyyy.MM.dd G 'at' HH:mm:ss z",
      "EEE, d MMM yyyy HH:mm:ss",
      "EEE, d MMM yyyy HH:mm:ss Z",
      "EEEEE MMMMM yyyy HH:mm:ss.SSSZ"
    };

    Locale dutch = new Locale("en", "US");

    for (String formatString : date_formats) {
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
    DateTimeParser[] parsers = {
      DateTimeFormat.forPattern( "yyyy-MM-dd HH" ).getParser(),
      DateTimeFormat.forPattern( "yyyy-MM-dd" ).getParser() };
    DateTimeFormatter formatter =
      new DateTimeFormatterBuilder().append( null, parsers )
        .toFormatter();
    return null;
  }
}

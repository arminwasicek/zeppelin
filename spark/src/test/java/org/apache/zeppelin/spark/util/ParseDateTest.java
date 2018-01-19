package org.apache.zeppelin.spark.util;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class ParseDateTest {
  @Test
  public void testDefault() {
    String examples[] = {
      "Jan 18, 2018 11:45",
      "Jan 18, 2018 12:00",
      "2018-01-18T14:37:23.556-08:00",
      "asd"};
    String expectedResult[] = {
      "2018-01-18T11:45:00.000-08:00",
      "2018-01-18T12:00:00.000-08:00",
      "2018-01-18T14:37:23.556-08:00",
      null};

    ArrayList<String> actualResult = new ArrayList<String>();;

    for (String e : examples) {
      DateTime d = ParseDate.parse(e);
      if (d!= null)
        actualResult.add(d.toString());
      else
        actualResult.add(null);
    }
    Assert.assertArrayEquals(actualResult.toArray(), expectedResult);
  }
}

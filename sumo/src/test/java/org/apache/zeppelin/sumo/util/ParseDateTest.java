package org.apache.zeppelin.sumo.util;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class ParseDateTest {
    String testDates2[] = {
//        "11/4/03 8:14 PM",
//        "8:14:11 PM",
          "2001-12-23",
          "2001/11/12",
          "2001 Dec 09",
          "2001 23 Jun",
          "12-12-2001",
          "01/01/2001",
          "02 Sep 2001",
          "02 Sep 2001",
          "2001-09-02 02:35",
          "2001-09-02 14:35 PST",
          "2001-09-02 02:35 am",
          "2001-09-02 02:35:14",
          "2001-09-02 02:35:14 PST",
          "2001-09-02 02:35:14 am",
          "2001-09-02 02:35:14 PM",
          "2001-09-02 02:35:14.002",
          "2001-09-02 02:35:14.002"};

    String testDates3[] = {
        "Tue Nov 04 20:14:11 EST 2003",
        "11/4/03 8:14 PM",
        "8:14:11 PM",
        "Nov 2, 2001 02:35:14",
        "November 2 2001 02:35:14 PDT",
        "2000/10/21",
        "Nov 4, 2003 8:14:11 PM",
        "8:14 PM",
        "8:14:11 PM",
        "8:14:11 PM EST",
        "11/4/03 8:14 PM",
        "Nov 4, 2003 8:14 PM",
        "November 4, 2003 8:14:11 PM EST"};

    @Test
    public void testParseDate() {
        String[] testDates = (String[]) ArrayUtils.addAll(
            testDates2, testDates3);
        boolean expectedResult[] = new boolean[testDates.length];
        boolean actualResult[] = new boolean[testDates.length];
        for (int i = 0; i < testDates.length; i++) {
            expectedResult[i] = true;
            if (ParseDate.parse(testDates[i]) == null) {
                actualResult[i] = false;
                System.out.println(testDates[i]);
            }
            else {
                actualResult[i] = true;
            }
        }
        assertArrayEquals(expectedResult, actualResult);
    }
}

package utils;

import net.egemsoft.rrd.utils.date.ConvertDate;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by cenk on 06/02/15.
 */
public class ConvertDateTest {
  @Test
  public void testParseStringToSeconds() {

    long value1 = ConvertDate.convertStringDateToSeconds("10s");
    long expectedValue1 = 10;
    assertEquals(expectedValue1, value1);

    long value2 = ConvertDate.convertStringDateToSeconds("5m");
    long expectedValue2 = 300;
    assertEquals(expectedValue2, value2);

    long value3 = ConvertDate.convertStringDateToSeconds("10h");
    long expectedValue3 = 36000;
    assertEquals(expectedValue3, value3);

    long value4 = ConvertDate.convertStringDateToSeconds("15d");
    long expectedValue4 = 1296000;
    assertEquals(expectedValue4, value4);

    long value5 = ConvertDate.convertStringDateToSeconds("3y");
    long expectedValue5 = 47304000;
    assertEquals(expectedValue5, value5);
  }
}
